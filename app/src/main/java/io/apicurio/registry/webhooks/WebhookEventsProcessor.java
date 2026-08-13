package io.apicurio.registry.webhooks;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.registry.events.dto.CloudEventConverter;
import io.apicurio.registry.events.dto.CloudEventDto;
import io.apicurio.registry.storage.dto.OutboxEvent;
import io.apicurio.registry.storage.impl.sql.SqlOutboxEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Turns a storage change into queued webhook deliveries.
 * <p>
 * This observer is invoked synchronously by CDI from inside {@code handles.withHandle(...)} in the SQL
 * storage layer, so the inserts below join that same transaction. That is intentional: a delivery is only
 * queued if the change that produced it actually commits.
 * <p>
 * The cost of that choice is that subscription lookup and the delivery inserts extend the storage
 * transaction. With a small number of subscriptions this is negligible, but it is the reason the actual
 * HTTP call happens later in {@link WebhookDispatcher} rather than here. Doing network I/O inside a
 * database transaction would hold a connection open for the duration of a remote call.
 */
@ApplicationScoped
public class WebhookEventsProcessor {

    @Inject
    Logger log;

    @Inject
    WebhookRepository repository;

    @Inject
    ObjectMapper mapper;

    @ConfigProperty(name = "apicurio.webhooks.enabled", defaultValue = "true")
    boolean enabled;

    @ConfigProperty(name = "apicurio.webhooks.source", defaultValue = "/apicurio-registry")
    String source;

    public void onStorageEvent(@Observes SqlOutboxEvent event) {
        if (!enabled) {
            return;
        }
        try {
            OutboxEvent outboxEvent = event.getOutboxEvent();
            CloudEventDto cloudEvent = CloudEventConverter.toCloudEvent(outboxEvent, source);
            if (cloudEvent == null) {
                // Not a data-change event (for example the READY signal), or an unmapped type.
                return;
            }

            List<WebhookSubscription> subscriptions = repository.listEnabledSubscriptions();
            if (subscriptions.isEmpty()) {
                return;
            }

            String payload = mapper.writeValueAsString(cloudEvent);
            long now = Instant.now().toEpochMilli();

            for (WebhookSubscription sub : subscriptions) {
                if (!matches(sub, cloudEvent.getType())) {
                    continue;
                }
                repository.enqueue(WebhookDelivery.builder()
                        .deliveryId(UUID.randomUUID().toString())
                        .subscriptionId(sub.getSubscriptionId())
                        .eventId(cloudEvent.getId())
                        .eventType(cloudEvent.getType())
                        .payload(payload)
                        .status(DeliveryStatus.PENDING)
                        .nextRetryAt(now)
                        .createdOn(now)
                        .build());
                log.debug("Queued webhook delivery of {} to subscription {}", cloudEvent.getType(),
                        sub.getSubscriptionId());
            }
        } catch (Exception ex) {
            // A webhook problem must never fail the registry operation that triggered it. Because this
            // observer runs inside the storage transaction, letting an exception escape would roll back
            // the artifact write itself.
            log.error("Failed to queue webhook deliveries for event {}", event.getOutboxEvent().getType(),
                    ex);
        }
    }

    /** An empty event type set means the subscription wants everything. */
    private static boolean matches(WebhookSubscription sub, String eventType) {
        return sub.getEventTypes() == null || sub.getEventTypes().isEmpty()
                || sub.getEventTypes().contains(eventType);
    }
}
