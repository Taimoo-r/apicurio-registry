package io.apicurio.registry.webhooks;

import io.apicurio.registry.storage.impl.sql.HandleFactory;
import io.apicurio.registry.storage.impl.sql.jdb.RowMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Persistence for webhook subscriptions and the durable delivery queue.
 * <p>
 * Two things about this class are deliberate.
 * <p>
 * First, it does not reuse the existing {@code outbox} table. That table is written and deleted in the same
 * transaction because it exists to feed a Debezium CDC connector reading the write-ahead log, so no row is
 * ever readable by the application. It is also only populated on PostgreSQL and SQL Server, per
 * {@code SqlEventRepository.supportsDatabaseEvents()}, and H2 has no such table at all. A webhook queue
 * built on it would work on half the supported dialects and would need a Kafka Connect deployment to work
 * at all.
 * <p>
 * Second, every method routes through {@link HandleFactory#withHandleNoException}, which is re-entrant: a
 * nested call joins the transaction already open on the calling thread instead of opening a new one. That
 * is what makes {@link #enqueue} atomic with the storage operation that triggered it, so an artifact insert
 * that rolls back cannot leave a queued delivery behind.
 * <p>
 * Note on style: the lambdas below are block-bodied because {@code withHandleNoException} is overloaded on
 * both a value-returning and a void-returning functional interface. An expression lambda whose body is a
 * method call is compatible with both, which makes the call ambiguous.
 * <p>
 * Known gap: {@code secret} is stored in plaintext. That is a deliberate scope boundary for this slice, not
 * an oversight; see the proposal for the encryption-at-rest options and their tradeoffs.
 */
@ApplicationScoped
public class WebhookRepository {

    private static final String INSERT_SUBSCRIPTION = "INSERT INTO webhook_subscriptions "
            + "(subscriptionId, endpointUrl, eventTypes, secret, enabled, createdOn) VALUES (?, ?, ?, ?, ?, ?)";

    private static final String SELECT_SUBSCRIPTIONS = "SELECT * FROM webhook_subscriptions ORDER BY createdOn";

    private static final String SELECT_ENABLED_SUBSCRIPTIONS = "SELECT * FROM webhook_subscriptions WHERE enabled = ?";

    private static final String SELECT_SUBSCRIPTION = "SELECT * FROM webhook_subscriptions WHERE subscriptionId = ?";

    private static final String INSERT_DELIVERY = "INSERT INTO webhook_deliveries "
            + "(deliveryId, subscriptionId, eventId, eventType, payload, status, attemptCount, nextRetryAt, "
            + "lastAttemptAt, claimedBy, claimedAt, httpStatusCode, errorMessage, createdOn) "
            + "VALUES (?, ?, ?, ?, ?, ?, 0, ?, 0, NULL, 0, NULL, NULL, ?)";

    /**
     * Note: {@code LIMIT} covers H2, PostgreSQL and MySQL but not SQL Server, which needs
     * {@code OFFSET ... FETCH NEXT}. Upstreaming this means moving the statement into
     * {@code SqlStatements} and overriding it in {@code SQLServerSqlStatements}, which is exactly why that
     * indirection exists. It is kept inline here to keep the slice reviewable in one file.
     */
    private static final String SELECT_DUE = "SELECT * FROM webhook_deliveries "
            + "WHERE status = ? AND nextRetryAt <= ? ORDER BY nextRetryAt LIMIT ?";

    private static final String SELECT_DELIVERIES_BY_SUBSCRIPTION = "SELECT * FROM webhook_deliveries "
            + "WHERE subscriptionId = ? ORDER BY createdOn";

    /**
     * Compare-and-swap claim. Only the replica whose UPDATE reports one affected row owns the delivery, so
     * two replicas polling the same batch cannot both dispatch it. This is portable across all four
     * supported dialects; {@code FOR UPDATE SKIP LOCKED} would reduce contention on PostgreSQL but is not
     * available everywhere and is a throughput optimization rather than a correctness requirement.
     */
    private static final String CLAIM_DELIVERY = "UPDATE webhook_deliveries "
            + "SET status = ?, claimedBy = ?, claimedAt = ? WHERE deliveryId = ? AND status = ?";

    private static final String MARK_DELIVERED = "UPDATE webhook_deliveries "
            + "SET status = ?, attemptCount = ?, lastAttemptAt = ?, httpStatusCode = ?, errorMessage = NULL, "
            + "claimedBy = NULL, claimedAt = 0 WHERE deliveryId = ?";

    private static final String MARK_RETRY = "UPDATE webhook_deliveries "
            + "SET status = ?, attemptCount = ?, lastAttemptAt = ?, nextRetryAt = ?, httpStatusCode = ?, "
            + "errorMessage = ?, claimedBy = NULL, claimedAt = 0 WHERE deliveryId = ?";

    /**
     * Returns any delivery still marked in-flight by a claim older than the cutoff back to the pending
     * queue. A replica that is killed mid-delivery leaves its claim behind; without this, that delivery
     * would never be retried and at-least-once would be violated by any hard shutdown.
     */
    private static final String RECLAIM_STALE = "UPDATE webhook_deliveries "
            + "SET status = ?, claimedBy = NULL, claimedAt = 0 WHERE status = ? AND claimedAt < ?";

    /** Shutdown counterpart to {@link #RECLAIM_STALE}, scoped to one replica's own claims. */
    private static final String RELEASE_BY_NODE = "UPDATE webhook_deliveries "
            + "SET status = ?, claimedBy = NULL, claimedAt = 0 WHERE status = ? AND claimedBy = ?";

    @Inject
    HandleFactory handles;

    public void createSubscription(WebhookSubscription sub) {
        handles.withHandleNoException(handle -> {
            handle.createUpdate(INSERT_SUBSCRIPTION)
                    .bind(0, sub.getSubscriptionId())
                    .bind(1, sub.getEndpointUrl())
                    .bind(2, joinEventTypes(sub.getEventTypes()))
                    .bind(3, sub.getSecret())
                    .bind(4, Boolean.valueOf(sub.isEnabled()))
                    .bind(5, Long.valueOf(sub.getCreatedOn()))
                    .execute();
        });
    }

    public List<WebhookSubscription> listSubscriptions() {
        return handles.withHandleNoException(handle -> {
            return handle.createQuery(SELECT_SUBSCRIPTIONS).map(SUBSCRIPTION_MAPPER).list();
        });
    }

    public List<WebhookSubscription> listEnabledSubscriptions() {
        return handles.withHandleNoException(handle -> {
            return handle.createQuery(SELECT_ENABLED_SUBSCRIPTIONS).bind(0, Boolean.TRUE)
                    .map(SUBSCRIPTION_MAPPER).list();
        });
    }

    public WebhookSubscription getSubscription(String subscriptionId) {
        return handles.withHandleNoException(handle -> {
            return handle.createQuery(SELECT_SUBSCRIPTION).bind(0, subscriptionId)
                    .map(SUBSCRIPTION_MAPPER).findOne().orElse(null);
        });
    }

    /**
     * Queues one delivery. Called from inside the storage transaction, so it commits or rolls back with the
     * change that produced the event.
     */
    public void enqueue(WebhookDelivery delivery) {
        handles.withHandleNoException(handle -> {
            handle.createUpdate(INSERT_DELIVERY)
                    .bind(0, delivery.getDeliveryId())
                    .bind(1, delivery.getSubscriptionId())
                    .bind(2, delivery.getEventId())
                    .bind(3, delivery.getEventType())
                    .bind(4, delivery.getPayload())
                    .bind(5, DeliveryStatus.PENDING.name())
                    .bind(6, Long.valueOf(delivery.getNextRetryAt()))
                    .bind(7, Long.valueOf(delivery.getCreatedOn()))
                    .execute();
        });
    }

    public List<WebhookDelivery> findDue(long now, int limit) {
        return handles.withHandleNoException(handle -> {
            return handle.createQuery(SELECT_DUE)
                    .bind(0, DeliveryStatus.PENDING.name())
                    .bind(1, Long.valueOf(now))
                    .bind(2, Integer.valueOf(limit))
                    .map(DELIVERY_MAPPER).list();
        });
    }

    public List<WebhookDelivery> listDeliveries(String subscriptionId) {
        return handles.withHandleNoException(handle -> {
            return handle.createQuery(SELECT_DELIVERIES_BY_SUBSCRIPTION).bind(0, subscriptionId)
                    .map(DELIVERY_MAPPER).list();
        });
    }

    /** @return true if this caller won the claim and may dispatch the delivery. */
    public boolean claim(String deliveryId, String nodeId, long now) {
        Integer updated = handles.withHandleNoException(handle -> {
            return handle.createUpdate(CLAIM_DELIVERY)
                    .bind(0, DeliveryStatus.IN_FLIGHT.name())
                    .bind(1, nodeId)
                    .bind(2, Long.valueOf(now))
                    .bind(3, deliveryId)
                    .bind(4, DeliveryStatus.PENDING.name())
                    .execute();
        });
        return updated != null && updated == 1;
    }

    public void markDelivered(String deliveryId, int attemptCount, long at, int httpStatus) {
        handles.withHandleNoException(handle -> {
            handle.createUpdate(MARK_DELIVERED)
                    .bind(0, DeliveryStatus.DELIVERED.name())
                    .bind(1, Integer.valueOf(attemptCount))
                    .bind(2, Long.valueOf(at))
                    .bind(3, Integer.valueOf(httpStatus))
                    .bind(4, deliveryId)
                    .execute();
        });
    }

    /**
     * Records a failed attempt. Passing {@link DeliveryStatus#PENDING} schedules another attempt at
     * {@code nextRetryAt}; passing {@link DeliveryStatus#FAILED} ends the sequence.
     */
    public void markAttemptFailed(String deliveryId, DeliveryStatus status, int attemptCount, long at,
            long nextRetryAt, Integer httpStatus, String error) {
        handles.withHandleNoException(handle -> {
            handle.createUpdate(MARK_RETRY)
                    .bind(0, status.name())
                    .bind(1, Integer.valueOf(attemptCount))
                    .bind(2, Long.valueOf(at))
                    .bind(3, Long.valueOf(nextRetryAt))
                    .bind(4, httpStatus)
                    .bind(5, truncate(error))
                    .bind(6, deliveryId)
                    .execute();
        });
    }

    /** @return how many of this replica's own claims were returned to the queue. */
    public int releaseClaimsForNode(String nodeId) {
        Integer updated = handles.withHandleNoException(handle -> {
            return handle.createUpdate(RELEASE_BY_NODE)
                    .bind(0, DeliveryStatus.PENDING.name())
                    .bind(1, DeliveryStatus.IN_FLIGHT.name())
                    .bind(2, nodeId)
                    .execute();
        });
        return updated == null ? 0 : updated;
    }

    /** @return how many stale claims were returned to the queue. */
    public int reclaimStale(long claimedBefore) {
        Integer updated = handles.withHandleNoException(handle -> {
            return handle.createUpdate(RECLAIM_STALE)
                    .bind(0, DeliveryStatus.PENDING.name())
                    .bind(1, DeliveryStatus.IN_FLIGHT.name())
                    .bind(2, Long.valueOf(claimedBefore))
                    .execute();
        });
        return updated == null ? 0 : updated;
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= 2000 ? error : error.substring(0, 2000);
    }

    private static String joinEventTypes(Set<String> types) {
        if (types == null || types.isEmpty()) {
            return "";
        }
        return String.join(",", types);
    }

    private static Set<String> splitEventTypes(String types) {
        if (types == null || types.isBlank()) {
            return new LinkedHashSet<>();
        }
        return Arrays.stream(types.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static final RowMapper<WebhookSubscription> SUBSCRIPTION_MAPPER = new RowMapper<>() {
        @Override
        public WebhookSubscription map(ResultSet rs) throws SQLException {
            return WebhookSubscription.builder()
                    .subscriptionId(rs.getString("subscriptionId"))
                    .endpointUrl(rs.getString("endpointUrl"))
                    .eventTypes(splitEventTypes(rs.getString("eventTypes")))
                    .secret(rs.getString("secret"))
                    .enabled(rs.getBoolean("enabled"))
                    .createdOn(rs.getLong("createdOn"))
                    .build();
        }
    };

    private static final RowMapper<WebhookDelivery> DELIVERY_MAPPER = new RowMapper<>() {
        @Override
        public WebhookDelivery map(ResultSet rs) throws SQLException {
            // wasNull() reports on the most recently read column, so it has to be checked immediately
            // after the getInt call rather than further down the builder chain.
            int httpStatus = rs.getInt("httpStatusCode");
            Integer httpStatusOrNull = rs.wasNull() ? null : httpStatus;
            return WebhookDelivery.builder()
                    .deliveryId(rs.getString("deliveryId"))
                    .subscriptionId(rs.getString("subscriptionId"))
                    .eventId(rs.getString("eventId"))
                    .eventType(rs.getString("eventType"))
                    .payload(rs.getString("payload"))
                    .status(DeliveryStatus.valueOf(rs.getString("status")))
                    .attemptCount(rs.getInt("attemptCount"))
                    .nextRetryAt(rs.getLong("nextRetryAt"))
                    .lastAttemptAt(rs.getLong("lastAttemptAt"))
                    .claimedBy(rs.getString("claimedBy"))
                    .claimedAt(rs.getLong("claimedAt"))
                    .httpStatusCode(httpStatusOrNull)
                    .errorMessage(rs.getString("errorMessage"))
                    .createdOn(rs.getLong("createdOn"))
                    .build();
        }
    };
}
