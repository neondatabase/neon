-- update approximately 4000 rows or 1 MB in the stripe_authorization_event_log table
-- takes about 5 minutes
UPDATE stripe.stripe_authorization_event_log
SET approved = NOT approved
WHERE ctid in (
    SELECT ctid
    FROM stripe.stripe_authorization_event_log
    TABLESAMPLE SYSTEM (0.0002) 
);