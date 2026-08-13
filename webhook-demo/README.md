# CloudEvents webhook delivery: end-to-end demo

A working vertical slice of [#8426](https://github.com/Apicurio/apicurio-registry/issues/8426) on the SQL
storage variant. One event type, one subscription, one delivery, through every layer.

The point of the demo is step 6. Everything before it is table stakes.

## What it proves

| Claim | Evidence |
|---|---|
| A subscription can be registered over REST | `POST /apis/registry/v3/admin/webhooks` returns 201 |
| A storage change emits a CloudEvent to that endpoint | attempt 1 arrives 0.9s after artifact creation |
| Payloads are signed and verifiable | `sig=valid` on every attempt |
| A rejecting endpoint causes backoff, not a hot loop | gaps of 1s, 2s, 4s, 8s |
| **Killing the registry mid-backoff does not lose the delivery** | **attempt 4 fires after restart, not attempt 1** |
| Retries eventually succeed and are recorded | `status=DELIVERED, attemptCount=5, httpStatusCode=204` |

## Recorded run

Registry on file-backed H2, receiver configured to reject the first 4 attempts.

```
12:13:55.226  artifact orders:1.0 created
12:13:56.125  REJECT 500   attempt=1   sig=valid   delivery 81fadb07
12:13:58.008  REJECT 500   attempt=2   sig=valid   delivery 81fadb07
12:14:01.007  REJECT 500   attempt=3   sig=valid   delivery 81fadb07
12:14:05.735  *** registry killed (docker rm -f) during backoff to attempt 4 ***
12:14:23.128  registry restarted against the same database
12:14:34.332  registry ready
12:14:33.497  REJECT 500   attempt=4   sig=valid   delivery 81fadb07
12:14:41.036  ACCEPT 204   attempt=5   sig=valid   delivery 81fadb07
```

Final state as reported by the registry:

```
deliveryId      81fadb07-89c4-4002-adb1-e94844068d35
eventType       io.apicurio.registry.artifact.version.created
status          DELIVERED
attemptCount    5
httpStatusCode  204
claimedBy       null
```

Two details in that log carry the whole argument.

**The attempt counter continued at 4.** The process that made attempts 1 through 3 no longer exists. The
counter is a column, so the replica that started afterwards knew exactly where the sequence was.

**The gap from attempt 4 to attempt 5 is 7.5 seconds.** That is the fourth step of the 1, 2, 4, 8 doubling
sequence. The backoff schedule survived the restart too, not just the fact that a delivery was outstanding.

This is the specific thing `HttpClientService`'s `@Retry(maxRetries = 8) @ExponentialBackoff` cannot do.
MicroProfile retry holds its state on the calling thread, so a `kill -9` loses every in-flight retry, and
the requirement in [#8569](https://github.com/Apicurio/apicurio-registry/issues/8569) is at-least-once
delivery with graceful shutdown.

## Running it

Requires Docker and Python 3. Build first:

```bash
mvn install -Dlocal -DskipTests
```

Then, in one terminal:

```bash
python webhook-demo/receiver.py --port 9099 --secret demo-secret --fail-first 4
```

In another, start the registry on a file-backed database (the default is in-memory H2, which would
defeat the restart step):

```bash
java -Dapicurio.datasource.url='jdbc:h2:file:/tmp/registry-demo;DB_CLOSE_DELAY=-1' -jar app/target/quarkus-app/quarkus-run.jar
```

Register a subscription, create an artifact, then kill and restart the registry while the retries are
still backing off. `webhook-demo/run-demo.sh` scripts the whole sequence against a container.

## Build notes

Two environment issues worth recording, neither specific to this change:

- Kiota aborts with exit code 134 in slim containers. Set `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1`.
- A clone made on Windows gives `mvnw` CRLF line endings, which makes it unrunnable inside Linux
  containers. Use the container's own `mvn`.

## Scope

In: schema for all four dialects, subscription persistence, event to delivery fan-out inside the storage
transaction, durable queue, compare-and-swap claiming, persisted exponential backoff, HMAC signing,
delivery log, minimal admin REST.

Out, deliberately: OpenAPI contract and generated SDK methods, subscription update and delete, group and
artifact filters, delivery log retention and purge, metrics, encryption at rest for the subscription
secret, and `FOR UPDATE SKIP LOCKED` as a PostgreSQL throughput optimization.
