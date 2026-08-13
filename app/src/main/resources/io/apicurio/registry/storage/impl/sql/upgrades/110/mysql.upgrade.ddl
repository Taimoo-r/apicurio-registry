-- *********************************************************************
-- DDL for the Apicurio Registry - Database: mysql
-- Upgrade Script from 109 to 110
-- *********************************************************************
CREATE TABLE webhook_subscriptions (subscriptionId VARCHAR(128) NOT NULL, endpointUrl VARCHAR(1024) NOT NULL, eventTypes VARCHAR(2048) NOT NULL, secret VARCHAR(512), enabled BOOLEAN NOT NULL DEFAULT TRUE, createdOn BIGINT NOT NULL, PRIMARY KEY (subscriptionId)) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE INDEX IDX_webhook_subscriptions_1 ON webhook_subscriptions(enabled);
CREATE TABLE webhook_deliveries (deliveryId VARCHAR(128) NOT NULL, subscriptionId VARCHAR(128) NOT NULL, eventId VARCHAR(128) NOT NULL, eventType VARCHAR(255) NOT NULL, payload TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, status VARCHAR(32) NOT NULL, attemptCount INT NOT NULL DEFAULT 0, nextRetryAt BIGINT NOT NULL DEFAULT 0, lastAttemptAt BIGINT NOT NULL DEFAULT 0, claimedBy VARCHAR(128), claimedAt BIGINT NOT NULL DEFAULT 0, httpStatusCode INT, errorMessage VARCHAR(2048), createdOn BIGINT NOT NULL, PRIMARY KEY (deliveryId), CONSTRAINT FK_webhook_deliveries_1 FOREIGN KEY (subscriptionId) REFERENCES webhook_subscriptions(subscriptionId) ON DELETE CASCADE, CONSTRAINT UQ_webhook_deliveries_1 UNIQUE (subscriptionId, eventId)) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE INDEX IDX_webhook_deliveries_1 ON webhook_deliveries(status, nextRetryAt);
UPDATE apicurio SET propValue = 110 WHERE propName = 'db_version';
