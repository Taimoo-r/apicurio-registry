package io.apicurio.registry.webhooks;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Set;

/**
 * A registered webhook endpoint and the set of CloudEvent types it wants to receive.
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class WebhookSubscription {

    private String subscriptionId;

    private String endpointUrl;

    /** CloudEvent type strings, e.g. {@code io.apicurio.registry.artifact.version.created}. */
    private Set<String> eventTypes;

    /**
     * Shared secret used to sign deliveries.
     * <p>
     * {@code @ToString.Exclude} keeps it out of logs and {@code @JsonIgnore} keeps it out of REST
     * responses. Without the latter, listing subscriptions hands every signing key to any caller who can
     * read the admin API, which is enough to forge deliveries to every registered endpoint. The row mapper
     * reads this column directly rather than through Jackson, so ignoring it for serialization does not
     * affect persistence.
     * <p>
     * It is still stored in plaintext; see the class-level note in {@link WebhookRepository}.
     */
    @ToString.Exclude
    @JsonIgnore
    private String secret;

    private boolean enabled;

    private long createdOn;
}
