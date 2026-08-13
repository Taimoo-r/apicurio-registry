package io.apicurio.registry.webhooks;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Signs webhook payloads so a receiver can verify the delivery actually came from this registry.
 * <p>
 * The signature is an HMAC-SHA256 over the raw request body, hex encoded, sent in the
 * {@code X-Registry-Signature} header as {@code sha256=<hex>}. Receivers must compare using a constant-time
 * comparison and must verify against the raw bytes, not a re-serialized form of the JSON.
 */
public final class WebhookSigner {

    public static final String SIGNATURE_HEADER = "X-Registry-Signature";

    private static final String ALGORITHM = "HmacSHA256";

    private WebhookSigner() {
    }

    /**
     * Computes the signature header value for a payload, or returns null when the subscription has no
     * secret configured (unsigned delivery).
     */
    public static String sign(String payload, String secret) {
        if (secret == null || secret.isBlank()) {
            return null;
        }
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            byte[] digest = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return "sha256=" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            // HmacSHA256 is required by the JDK, so this cannot happen with a non-empty key.
            throw new IllegalStateException("Unable to sign webhook payload", e);
        }
    }
}
