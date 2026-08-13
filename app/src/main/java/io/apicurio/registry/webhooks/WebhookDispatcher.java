package io.apicurio.registry.webhooks;

import io.apicurio.registry.storage.RegistryStorage;
import io.apicurio.registry.cdi.Current;
import io.quarkus.scheduler.Scheduled;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP;

/**
 * Delivers queued webhook events, with retry state held in the database rather than in memory.
 * <p>
 * The obvious implementation is to reuse {@code HttpClientService}, which already carries
 * {@code @Retry(maxRetries = 8) @ExponentialBackoff}. That does not work for this requirement. MicroProfile
 * retry is in-thread: a backoff that grows to five minutes would pin a worker thread for five minutes, and
 * every in-flight retry is lost when the process restarts. At-least-once delivery with graceful shutdown
 * means the schedule has to outlive the JVM, which means it has to be a column.
 * <p>
 * Each tick does three things: return stale claims to the queue, take due deliveries, and attempt them.
 */
@ApplicationScoped
public class WebhookDispatcher {

    /** Identifies this replica in the {@code claimedBy} column. */
    private final String nodeId = UUID.randomUUID().toString();

    @Inject
    Logger log;

    @Inject
    WebhookRepository repository;

    @Inject
    WebClient webClient;

    @Inject
    @Current
    RegistryStorage storage;

    @ConfigProperty(name = "apicurio.webhooks.enabled", defaultValue = "true")
    boolean enabled;

    @ConfigProperty(name = "apicurio.webhooks.batch-size", defaultValue = "50")
    int batchSize;

    @ConfigProperty(name = "apicurio.webhooks.retry.max-attempts", defaultValue = "10")
    int maxAttempts;

    @ConfigProperty(name = "apicurio.webhooks.retry.initial-delay-ms", defaultValue = "1000")
    long initialDelayMs;

    @ConfigProperty(name = "apicurio.webhooks.retry.max-delay-ms", defaultValue = "300000")
    long maxDelayMs;

    @ConfigProperty(name = "apicurio.webhooks.claim-timeout-ms", defaultValue = "60000")
    long claimTimeoutMs;

    @ConfigProperty(name = "apicurio.webhooks.request-timeout-ms", defaultValue = "10000")
    long requestTimeoutMs;

    @Scheduled(every = "{apicurio.webhooks.dispatcher.every}", concurrentExecution = SKIP)
    void dispatch() {
        if (!enabled) {
            return;
        }
        try {
            if (!storage.isReady() || storage.isReadOnly()) {
                return;
            }

            long now = Instant.now().toEpochMilli();

            // A replica that died mid-delivery left its claim behind. Reclaiming is what allows a killed
            // process to resume rather than stranding the delivery in IN_FLIGHT forever.
            int reclaimed = repository.reclaimStale(now - claimTimeoutMs);
            if (reclaimed > 0) {
                log.info("Returned {} stale webhook delivery claim(s) to the queue", reclaimed);
            }

            List<WebhookDelivery> due = repository.findDue(now, batchSize);
            for (WebhookDelivery delivery : due) {
                // Compare-and-swap: if another replica already took this row, skip it.
                if (!repository.claim(delivery.getDeliveryId(), nodeId, Instant.now().toEpochMilli())) {
                    continue;
                }
                attempt(delivery);
            }
        } catch (Exception ex) {
            log.error("Webhook dispatcher tick failed", ex);
        }
    }

    private void attempt(WebhookDelivery delivery) {
        int attemptCount = delivery.getAttemptCount() + 1;
        long at = Instant.now().toEpochMilli();

        WebhookSubscription sub = repository.getSubscription(delivery.getSubscriptionId());
        if (sub == null || !sub.isEnabled()) {
            repository.markAttemptFailed(delivery.getDeliveryId(), DeliveryStatus.FAILED, attemptCount, at,
                    0, null, "Subscription missing or disabled");
            return;
        }

        try {
            var request = webClient.postAbs(sub.getEndpointUrl())
                    .putHeader("Content-Type", "application/cloudevents+json; charset=UTF-8")
                    .putHeader("X-Registry-Event-Id", delivery.getEventId())
                    .putHeader("X-Registry-Event-Type", delivery.getEventType())
                    .putHeader("X-Registry-Delivery-Id", delivery.getDeliveryId())
                    .putHeader("X-Registry-Delivery-Attempt", Integer.toString(attemptCount))
                    .timeout(requestTimeoutMs);

            String signature = WebhookSigner.sign(delivery.getPayload(), sub.getSecret());
            if (signature != null) {
                request.putHeader(WebhookSigner.SIGNATURE_HEADER, signature);
            }

            HttpResponse<Buffer> response = request.sendBuffer(Buffer.buffer(delivery.getPayload()))
                    .toCompletionStage().toCompletableFuture()
                    .get(requestTimeoutMs + 5000, TimeUnit.MILLISECONDS);

            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                repository.markDelivered(delivery.getDeliveryId(), attemptCount, at, status);
                log.debug("Delivered {} to {} on attempt {}", delivery.getEventId(), sub.getEndpointUrl(),
                        attemptCount);
                return;
            }
            if (isRetriable(status)) {
                reschedule(delivery, attemptCount, at, status, "HTTP " + status);
            } else {
                // A 4xx means the receiver understood and rejected the request. Retrying an identical
                // payload will produce an identical rejection, so this ends the sequence.
                repository.markAttemptFailed(delivery.getDeliveryId(), DeliveryStatus.FAILED, attemptCount,
                        at, 0, status, "Non-retriable HTTP " + status);
                log.warn("Delivery {} failed permanently with HTTP {}", delivery.getDeliveryId(), status);
            }
        } catch (Exception ex) {
            reschedule(delivery, attemptCount, at, null, ex.getClass().getSimpleName() + ": "
                    + ex.getMessage());
        }
    }

    private void reschedule(WebhookDelivery delivery, int attemptCount, long at, Integer status,
            String error) {
        if (attemptCount >= maxAttempts) {
            repository.markAttemptFailed(delivery.getDeliveryId(), DeliveryStatus.FAILED, attemptCount, at,
                    0, status, "Retries exhausted after " + attemptCount + " attempts: " + error);
            log.warn("Delivery {} exhausted {} attempts, giving up", delivery.getDeliveryId(), attemptCount);
            return;
        }
        long delay = backoffMs(attemptCount);
        repository.markAttemptFailed(delivery.getDeliveryId(), DeliveryStatus.PENDING, attemptCount, at,
                at + delay, status, error);
        log.debug("Delivery {} attempt {} failed ({}), retrying in {}ms", delivery.getDeliveryId(),
                attemptCount, error, delay);
    }

    /** Exponential backoff, doubling from the initial delay and clamped at the configured maximum. */
    long backoffMs(int attemptCount) {
        long delay = initialDelayMs;
        for (int i = 1; i < attemptCount && delay < maxDelayMs; i++) {
            delay = delay * 2;
        }
        return Math.min(delay, maxDelayMs);
    }

    /**
     * 408 and 429 are the two 4xx codes that describe a transient condition, so they are retried alongside
     * 5xx. Everything else in the 4xx range is treated as a permanent rejection.
     */
    private static boolean isRetriable(int status) {
        return status >= 500 || status == 408 || status == 429;
    }

    /**
     * On an orderly shutdown, hand back anything this replica is holding so another replica picks it up
     * immediately instead of waiting out the claim timeout.
     */
    @PreDestroy
    void releaseClaims() {
        if (!enabled) {
            return;
        }
        try {
            int released = repository.releaseClaimsForNode(nodeId);
            if (released > 0) {
                log.info("Released {} webhook delivery claim(s) on shutdown", released);
            }
        } catch (Exception ex) {
            log.debug("Could not release webhook claims on shutdown", ex);
        }
    }
}
