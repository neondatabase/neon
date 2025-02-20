\set event_type random(1,10)
\set service_key random(1, 3)

INSERT INTO webhook.incoming_webhooks (
    created_at, 
    delivery_id, 
    upstream_emitted_at, 
    service_key, 
    event_id, 
    source, 
    body, 
    json, 
    additional_data, 
    is_body_encrypted, 
    event_type
) VALUES (
    now(),
    gen_random_uuid(),
    now() - interval '10 minutes',
    CASE :service_key::int
        WHEN 1 THEN 'shopify'
        WHEN 2 THEN 'stripe'
        WHEN 3 THEN 'github'
    END,
    'evt_' || gen_random_uuid(),  -- Ensures uniqueness
    CASE :service_key::int
        WHEN 1 THEN 'Shopify'
        WHEN 2 THEN 'Stripe'
        WHEN 3 THEN 'GitHub'
    END,
    '{"order_id": 987654, "customer": {"name": "John Doe", "email": "john.doe@example.com"}, "items": [{"product_id": 12345, "quantity": 2}, {"product_id": 67890, "quantity": 1}], "total": 199.99}',
    '{"order_id": 987654, "customer": {"name": "John Doe", "email": "john.doe@example.com"}, "items": [{"product_id": 12345, "quantity": 2}, {"product_id": 67890, "quantity": 1}], "total": 199.99}'::jsonb,
    '{"metadata": {"user_agent": "Mozilla/5.0", "ip_address": "203.0.113.42"}}'::jsonb,
    false,
    CASE :event_type::int
        WHEN 1 THEN 'ORDER_PLACED'
        WHEN 2 THEN 'ORDER_CANCELLED'
        WHEN 3 THEN 'PAYMENT_SUCCESSFUL'
        WHEN 4 THEN 'PAYMENT_FAILED'
        WHEN 5 THEN 'CUSTOMER_CREATED'
        WHEN 6 THEN 'CUSTOMER_UPDATED'
        WHEN 7 THEN 'PRODUCT_UPDATED'
        WHEN 8 THEN 'INVENTORY_LOW'
        WHEN 9 THEN 'SHIPPING_DISPATCHED'
        WHEN 10 THEN 'REFUND_ISSUED'
    END
);