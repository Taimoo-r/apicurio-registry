package io.apicurio.registry.webhooks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the two pieces of webhook logic that are pure functions: the retry schedule and the payload
 * signature. Everything else in the delivery path needs a real database and a real endpoint, and is
 * covered by the end-to-end demo rather than by mocks that would only assert the implementation back to
 * itself.
 */
class WebhookDeliveryLogicTest {

    private WebhookDispatcher dispatcherWith(long initial, long max) {
        WebhookDispatcher d = new WebhookDispatcher();
        d.initialDelayMs = initial;
        d.maxDelayMs = max;
        return d;
    }

    @Test
    void backoffDoublesFromTheInitialDelay() {
        WebhookDispatcher d = dispatcherWith(1000, 300000);

        assertEquals(1000, d.backoffMs(1));
        assertEquals(2000, d.backoffMs(2));
        assertEquals(4000, d.backoffMs(3));
        assertEquals(8000, d.backoffMs(4));
    }

    @Test
    void backoffIsClampedAtTheConfiguredMaximum() {
        WebhookDispatcher d = dispatcherWith(1000, 300000);

        // 1s doubling reaches 5 minutes at attempt 10, and must not exceed it afterwards.
        assertEquals(300000, d.backoffMs(10));
        assertEquals(300000, d.backoffMs(20));
        assertTrue(d.backoffMs(50) <= 300000);
    }

    @Test
    void signatureIsStableAndKeyDependent() {
        String payload = "{\"id\":\"abc\",\"type\":\"io.apicurio.registry.artifact.created\"}";

        String a = WebhookSigner.sign(payload, "secret-one");
        String b = WebhookSigner.sign(payload, "secret-one");
        String c = WebhookSigner.sign(payload, "secret-two");

        assertEquals(a, b);
        assertNotEquals(a, c);
        assertTrue(a.startsWith("sha256="));
    }

    @Test
    void signatureChangesWhenThePayloadChanges() {
        String secret = "shared";

        String a = WebhookSigner.sign("{\"v\":1}", secret);
        String b = WebhookSigner.sign("{\"v\":2}", secret);

        assertNotEquals(a, b);
    }

    @Test
    void unsignedWhenNoSecretConfigured() {
        assertNull(WebhookSigner.sign("{}", null));
        assertNull(WebhookSigner.sign("{}", "   "));
    }
}
