-- *********************************************************************
-- DDL for the Apicurio Registry - Database: h2
-- Upgrade Script from 109 to 110
-- *********************************************************************
CREATE TABLE webhook_subscriptions (subscriptionId VARCHAR(128) NOT NULL, endpointUrl VARCHAR(1024) NOT NULL, eventTypes VARCHAR(2048) NOT NULL, secret VARCHAR(512), enabled BOOLEAN NOT NULL DEFAULT TRUE, createdOn BIGINT NOT NULL);
ALTER TABLE webhook_subscriptions ADD PRIMARY KEY (subscriptionId);
CREATE INDEX IDX_webhook_subscriptions_1 ON webhook_subscriptions(enabled);
CREATE TABLE webhook_deliveries (deliveryId VARCHAR(128) NOT NULL, subscriptionId VARCHAR(128) NOT NULL, eventId VARCHAR(128) NOT NULL, eventType VARCHAR(255) NOT NULL, payload TEXT NOT NULL, status VARCHAR(32) NOT NULL, attemptCount INT NOT NULL DEFAULT 0, nextRetryAt BIGINT NOT NULL DEFAULT 0, lastAttemptAt BIGINT NOT NULL DEFAULT 0, claimedBy VARCHAR(128), claimedAt BIGINT NOT NULL DEFAULT 0, httpStatusCode INT, errorMessage VARCHAR(2048), createdOn BIGINT NOT NULL);
ALTER TABLE webhook_deliveries ADD PRIMARY KEY (deliveryId);
ALTER TABLE webhook_deliveries ADD CONSTRAINT FK_webhook_deliveries_1 FOREIGN KEY (subscriptionId) REFERENCES webhook_subscriptions(subscriptionId) ON DELETE CASCADE;
ALTER TABLE webhook_deliveries ADD CONSTRAINT UQ_webhook_deliveries_1 UNIQUE (subscriptionId, eventId);
CREATE INDEX IDX_webhook_deliveries_1 ON webhook_deliveries(status, nextRetryAt);
UPDATE apicurio SET propValue = 110 WHERE propName = 'db_version';
