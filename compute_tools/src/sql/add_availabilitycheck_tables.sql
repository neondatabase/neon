DO $$
BEGIN
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_tables
        WHERE tablename = 'health_check'
    )
    THEN
    CREATE TABLE health_check (
        id serial primary key,
        updated_at timestamptz default now()
    );
    INSERT INTO health_check VALUES (1, now())
        ON CONFLICT (id) DO UPDATE
         SET updated_at = now();
    END IF;
END
$$