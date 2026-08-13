-- *********************************************************************
-- DDL for the Apicurio Registry - Database: mssql
-- Upgrade Script from 109 to 110
-- *********************************************************************
CREATE TABLE webhook_subscriptions (subscriptionId NVARCHAR(128) NOT NULL, endpointUrl NVARCHAR(1024) NOT NULL, eventTypes NVARCHAR(2048) NOT NULL, secret NVARCHAR(512), enabled BIT NOT NULL DEFAULT 1, createdOn BIGINT NOT NULL);
ALTER TABLE webhook_subscriptions ADD PRIMARY KEY (subscriptionId);
CREATE INDEX IDX_webhook_subscriptions_1 ON webhook_subscriptions(enabled);
CREATE TABLE webhook_deliveries (deliveryId NVARCHAR(128) NOT NULL, subscriptionId NVARCHAR(128) NOT NULL, eventId NVARCHAR(128) NOT NULL, eventType NVARCHAR(255) NOT NULL, payload NVARCHAR(MAX) NOT NULL, status NVARCHAR(32) NOT NULL, attemptCount INT NOT NULL DEFAULT 0, nextRetryAt BIGINT NOT NULL DEFAULT 0, lastAttemptAt BIGINT NOT NULL DEFAULT 0, claimedBy NVARCHAR(128), claimedAt BIGINT NOT NULL DEFAULT 0, httpStatusCode INT, errorMessage NVARCHAR(2048), createdOn BIGINT NOT NULL);
ALTER TABLE webhook_deliveries ADD PRIMARY KEY (deliveryId);
ALTER TABLE webhook_deliveries ADD CONSTRAINT FK_webhook_deliveries_1 FOREIGN KEY (subscriptionId) REFERENCES webhook_subscriptions(subscriptionId) ON DELETE CASCADE;
ALTER TABLE webhook_deliveries ADD CONSTRAINT UQ_webhook_deliveries_1 UNIQUE (subscriptionId, eventId);
CREATE INDEX IDX_webhook_deliveries_1 ON webhook_deliveries(status, nextRetryAt);
UPDATE apicurio SET propValue = 110 WHERE propName = 'db_version';
