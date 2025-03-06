-- select one of the most recent webhook records (created in the branch timeline during the bench run)
SELECT * 
FROM webhook.incoming_webhooks
WHERE id = (
    SELECT (floor(random() * (
        (SELECT last_value FROM webhook.incoming_webhooks_id_seq) - 1350000001 + 1
    ) + 1350000001))::bigint
)
LIMIT 1;