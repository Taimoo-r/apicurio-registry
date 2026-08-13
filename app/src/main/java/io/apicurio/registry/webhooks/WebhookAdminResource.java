package io.apicurio.registry.webhooks;

import io.apicurio.registry.logging.Logged;
import io.apicurio.registry.auth.Authorized;
import io.apicurio.registry.auth.AuthorizedLevel;
import io.apicurio.registry.auth.AuthorizedStyle;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Minimal subscription management for the webhook slice.
 * <p>
 * Upstreaming this means declaring the endpoints in {@code common/src/main/resources/META-INF/openapi.json}
 * and implementing the generated interface, the way the rest of the v3 API works. It is hand-written here
 * because regenerating the client SDKs is not what this slice is trying to demonstrate.
 */
@ApplicationScoped
@Logged
@Path("/apis/registry/v3/admin/webhooks")
public class WebhookAdminResource {

    @Inject
    WebhookRepository repository;

    @Authorized(style = AuthorizedStyle.None, level = AuthorizedLevel.Admin)
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(CreateWebhookRequest request) {
        if (request == null || request.endpointUrl == null || request.endpointUrl.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("endpointUrl is required")).build();
        }
        // Reject anything that is not a usable absolute HTTP(S) URL up front, rather than discovering it
        // once per retry in the dispatcher.
        try {
            URI uri = new URI(request.endpointUrl);
            if (!uri.isAbsolute() || uri.getHost() == null
                    || !(uri.getScheme().equals("http") || uri.getScheme().equals("https"))) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorMessage("endpointUrl must be an absolute http or https URL"))
                        .build();
            }
        } catch (URISyntaxException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("endpointUrl is not a valid URI")).build();
        }

        WebhookSubscription sub = WebhookSubscription.builder()
                .subscriptionId(UUID.randomUUID().toString())
                .endpointUrl(request.endpointUrl)
                .eventTypes(request.eventTypes == null ? new LinkedHashSet<>()
                        : new LinkedHashSet<>(request.eventTypes))
                .secret(request.secret)
                .enabled(true)
                .createdOn(Instant.now().toEpochMilli())
                .build();
        repository.createSubscription(sub);
        return Response.status(Response.Status.CREATED).entity(sub).build();
    }

    @Authorized(style = AuthorizedStyle.None, level = AuthorizedLevel.Admin)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<WebhookSubscription> list() {
        return repository.listSubscriptions();
    }

    @Authorized(style = AuthorizedStyle.None, level = AuthorizedLevel.Admin)
    @GET
    @Path("{subscriptionId}/deliveries")
    @Produces(MediaType.APPLICATION_JSON)
    public List<WebhookDelivery> deliveries(@PathParam("subscriptionId") String subscriptionId) {
        return repository.listDeliveries(subscriptionId);
    }

    public static class CreateWebhookRequest {
        public String endpointUrl;
        public Set<String> eventTypes;
        public String secret;
    }

    public static class ErrorMessage {
        public String message;

        public ErrorMessage() {
        }

        public ErrorMessage(String message) {
            this.message = message;
        }
    }
}
