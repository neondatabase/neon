-- Zipfian distributions model real-world access patterns where:
--	A few values (popular IDs) are accessed frequently.
--	Many values are accessed rarely.
-- This is useful for simulating realistic workloads, like webhook processing where recent events are more frequently accessed.

\set alpha 1.2  
\set min_id 1
\set max_id 135000000

\set zipf_random_id random_zipfian(:min_id, :max_id, :alpha)

SELECT * 
FROM webhook.incoming_webhooks
WHERE id = (:zipf_random_id)::bigint
LIMIT 1;