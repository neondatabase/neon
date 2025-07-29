DO $$
BEGIN
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_tables
        WHERE tablename::pg_catalog.name OPERATOR(pg_catalog.=) 'health_check'::pg_catalog.name
        AND schemaname::pg_catalog.name OPERATOR(pg_catalog.=) 'public'::pg_catalog.name
    )
    THEN
    CREATE TABLE public.health_check (
        id pg_catalog.int4 primary key generated as identity,
        updated_at pg_catalog.timestamptz default pg_catalog.now()
    );
    INSERT INTO public.health_check VALUES (1, pg_catalog.now())
        ON CONFLICT (id) DO UPDATE
         SET updated_at = pg_catalog.now();
    END IF;
END
$$