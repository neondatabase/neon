-- add 100000 rows or approx. 1 MB to the relabeled_transactions table
-- takes about 1 second
INSERT INTO heron.relabeled_transactions (
    id, 
    created_at, 
    universal_transaction_id, 
    raw_result, 
    category, 
    category_confidence, 
    merchant, 
    batch_id
)
SELECT 
    gs.aid AS id, 
    now() - (gs.aid % 1000) * interval '1 second' AS created_at, 
    'txn_' || gs.aid AS universal_transaction_id, 
    '{}'::jsonb AS raw_result, 
    CASE WHEN gs.aid % 5 = 0 THEN 'grocery' 
         WHEN gs.aid % 5 = 1 THEN 'electronics' 
         WHEN gs.aid % 5 = 2 THEN 'clothing' 
         WHEN gs.aid % 5 = 3 THEN 'utilities' 
         ELSE NULL END AS category, 
    ROUND(RANDOM()::numeric, 2) AS category_confidence, 
    CASE WHEN gs.aid % 2 = 0 THEN 'Merchant_' || gs.aid % 20 ELSE NULL END AS merchant, 
    gs.aid % 100 + 1 AS batch_id
FROM generate_series(1, 100000) AS gs(aid);