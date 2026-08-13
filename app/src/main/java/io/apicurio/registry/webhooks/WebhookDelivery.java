package io.apicurio.registry.webhooks;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * One event queued for delivery to one subscription.
 * <p>
 * The retry state ({@code attemptCount}, {@code nextRetryAt}) lives in the database rather than in memory,
 * so a delivery that is mid-backoff when the process dies is picked up again by whichever replica starts
 * next. This is what makes at-least-once delivery survive a restart.
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class WebhookDelivery {

    private String deliveryId;

    private String subscriptionId;

    /** The CloudEvent {@code id}. Unique per (subscription, event) so replays are idempotent. */
    private String eventId;

    /** The CloudEvent {@code type}. */
    private String eventType;

    /** The serialized CloudEvent, stored so delivery does not depend on re-reading registry state. */
    private String payload;

    private DeliveryStatus status;

    private int attemptCount;

    /** Epoch millis after which this delivery is eligible for dispatch. */
    private long nextRetryAt;

    /** Epoch millis of the last attempt, or 0 if never attempted. */
    private long lastAttemptAt;

    /** Identifies the replica holding the current claim, null when unclaimed. */
    private String claimedBy;

    /** Epoch millis when the current claim was taken, used to detect stale claims. */
    private long claimedAt;

    private Integer httpStatusCode;

    private String errorMessage;

    private long createdOn;
}
