-- update approximately 2000 rows or 1 MB in the incoming_webhooks table
-- takes about 5 seconds
UPDATE webhook.incoming_webhooks
SET is_body_encrypted = NOT is_body_encrypted
WHERE ctid in (
    SELECT ctid
    FROM webhook.incoming_webhooks
    TABLESAMPLE SYSTEM (0.0002) 
);
