package io.apicurio.registry.webhooks;

/**
 * Lifecycle of a single webhook delivery attempt sequence.
 * <p>
 * The status column is the coordination point between replicas: a delivery is only ever dispatched by the
 * replica that successfully transitions it from {@link #PENDING} to {@link #IN_FLIGHT}.
 */
public enum DeliveryStatus {

    /** Enqueued and eligible for dispatch once {@code nextRetryAt} has passed. */
    PENDING,

    /** Claimed by a replica and currently being delivered. Reset to PENDING if the claim goes stale. */
    IN_FLIGHT,

    /** The endpoint accepted the event with a 2xx response. Terminal. */
    DELIVERED,

    /** Retries were exhausted, or the endpoint rejected the event with a non-retriable status. Terminal. */
    FAILED
}
