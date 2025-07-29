DO $$
BEGIN
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_tables
        WHERE tablename OPERATOR(pg_catalog.=) 'drop_subscriptions_done'::pg_catalog.name
        AND schemaname OPERATOR(pg_catalog.=) 'neon'::pg_catalog.name
    )
    THEN
        CREATE TABLE neon.drop_subscriptions_done
        (id pg_catalog.int4 primary key generated as identity, timeline_id pg_catalog.text);
    END IF;

    -- preserve the timeline_id of the last drop_subscriptions run
    -- to ensure that the cleanup of a timeline is executed only once.
    -- use upsert to avoid the table bloat in case of cascade branching (branch of a branch)
    INSERT INTO neon.drop_subscriptions_done VALUES (1, pg_catalog.current_setting('neon.timeline_id'))
    ON CONFLICT (id) DO UPDATE
    SET timeline_id::pg_catalog.text OPERATOR(pg_catalog.=) pg_catalog.current_setting('neon.timeline_id')::pg_catalog.text;
END
$$
