-- update, insert and delete one row each, to keep the size of the table about the same

\set alpha 1.2
\set min_id 1
\set max_id 1500000000
\set range_size 100

\set zipf_random_id random_zipfian(:min_id, :max_id, :alpha)
\set random_mar_id random(1, 65536)

\set zipf_random_delete_id random_zipfian(:min_id, :max_id, :alpha)

UPDATE transaction.transaction
SET state = 'COMPLETED',
    settlement_date = CURRENT_DATE,
    mar_identifier = (:random_mar_id)::int
WHERE id = (:zipf_random_id)::bigint;

INSERT INTO transaction.transaction (
    -- Omit `id` so the default sequence is used
    user_id,
    card_id,
    business_id,
    preceding_transaction_id,
    is_last,
    is_mocked,
    type,
    state,
    network,
    subnetwork,
    user_transaction_time,
    settlement_date,
    request_amount,
    amount,
    currency_code,
    approval_code,
    response,
    gpa,
    gpa_order_unload,
    gpa_order,
    program_transfer,
    fee_transfer,
    peer_transfer,
    msa_orders,
    risk_assessment,
    auto_reload,
    direct_deposit,
    polarity,
    real_time_fee_group,
    fee,
    chargeback,
    standin_approved_by,
    acquirer_fee_amount,
    funded_account_holder,
    digital_wallet_token,
    network_fees,
    card_security_code_verification,
    fraud,
    cardholder_authentication_data,
    currency_conversion,
    merchant,
    store,
    card_acceptor,
    acquirer,
    pos,
    avs,
    mar_token,
    mar_preceding_related_transaction_token,
    mar_business_token,
    mar_acting_user_token,
    mar_card_token,
    mar_duration,
    mar_created_time,
    issuer_interchange_amount,
    offer_orders,
    transaction_canonical_id,
    mar_identifier,
    created_at,
    card_acceptor_mid,
    card_acceptor_name,
    address_verification,
    issuing_product,
    mar_enhanced_data_token,
    standin_reason
)
SELECT
    -- Keep user_id in [1..100000]
    (:zipf_random_id % 100000) + 1 AS user_id,
    -- Keep card_id in [1..500000]
    (:zipf_random_id % 500000) + 1  AS card_id,
    -- Keep business_id in [1..20000]
    (:zipf_random_id % 20000) + 1  AS business_id,
    NULL AS preceding_transaction_id,
    -- Booleans by mod
    (:zipf_random_id % 2) = 0  AS is_last,
    (:zipf_random_id % 5) = 0  AS is_mocked,
    'authorization'            AS type,
    'PENDING'                  AS state,
    'VISA'                     AS network,
    'VISANET'                  AS subnetwork,
    now() - ((:zipf_random_id % 100) || ' days')::interval AS user_transaction_time,
    now() - ((:zipf_random_id % 100) || ' days')::interval AS settlement_date,
    random() * 1000            AS request_amount,
    random() * 1000            AS amount,
    'USD'                      AS currency_code,
    md5((:zipf_random_id)::text) AS approval_code,
    '{}'::jsonb                AS response,
    '{}'::jsonb                AS gpa,
    '{}'::jsonb                AS gpa_order_unload,
    '{}'::jsonb                AS gpa_order,
    '{}'::jsonb                AS program_transfer,
    '{}'::jsonb                AS fee_transfer,
    '{}'::jsonb                AS peer_transfer,
    '{}'::jsonb                AS msa_orders,
    '{}'::jsonb                AS risk_assessment,
    '{}'::jsonb                AS auto_reload,
    '{}'::jsonb                AS direct_deposit,
    '{}'::jsonb                AS polarity,
    '{}'::jsonb                AS real_time_fee_group,
    '{}'::jsonb                AS fee,
    '{}'::jsonb                AS chargeback,
    NULL                       AS standin_approved_by,
    random() * 100             AS acquirer_fee_amount,
    '{}'::jsonb                AS funded_account_holder,
    '{}'::jsonb                AS digital_wallet_token,
    '{}'::jsonb                AS network_fees,
    '{}'::jsonb                AS card_security_code_verification,
    '{}'::jsonb                AS fraud,
    '{}'::jsonb                AS cardholder_authentication_data,
    '{}'::jsonb                AS currency_conversion,
    '{}'::jsonb                AS merchant,
    '{}'::jsonb                AS store,
    '{}'::jsonb                AS card_acceptor,
    '{}'::jsonb                AS acquirer,
    '{}'::jsonb                AS pos,
    '{}'::jsonb                AS avs,
    md5((:zipf_random_id)::text || 'token') AS mar_token,
    NULL AS mar_preceding_related_transaction_token,
    NULL AS mar_business_token,
    NULL AS mar_acting_user_token,
    NULL AS mar_card_token,
    random() * 1000            AS mar_duration,
    now()                      AS mar_created_time,
    random() * 100             AS issuer_interchange_amount,
    '{}'::jsonb                AS offer_orders,
    -- Keep transaction_canonical_id in [1..500]
    (:zipf_random_id % 500) + 1 AS transaction_canonical_id,
    -- Use the raw zipf_random_id as mar_identifier
    :zipf_random_id::integer   AS mar_identifier,
    now()                      AS created_at,
    NULL                       AS card_acceptor_mid,
    NULL                       AS card_acceptor_name,
    '{}'::jsonb                AS address_verification,
    'DEFAULT_PRODUCT'          AS issuing_product,
    NULL                       AS mar_enhanced_data_token,
    NULL                       AS standin_reason
FROM (SELECT 1) AS dummy;  -- Forces a single row


-- delete exactly one row

WITH to_delete AS (
    SELECT id
    FROM transaction.transaction
    WHERE id >= (:zipf_random_delete_id)::bigint
      AND id < ((:zipf_random_delete_id)::bigint + :range_size)
    ORDER BY id
    LIMIT 1
)
DELETE FROM transaction.transaction
USING to_delete
WHERE transaction.transaction.id = to_delete.id;