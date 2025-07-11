-- update approximately 6000 rows or 600 kb in the raw_finicity_transaction table
-- takes about 1 second
UPDATE banking.raw_finicity_transaction
SET raw_data = 
    jsonb_set(
        raw_data,
        '{updated}',
        to_jsonb(now()),
        true
    )
WHERE ctid IN (
    SELECT ctid
    FROM banking.raw_finicity_transaction
    TABLESAMPLE SYSTEM (0.0005)
);
