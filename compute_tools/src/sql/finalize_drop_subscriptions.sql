DO $$
BEGIN
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_tables
        WHERE tablename = 'drop_subscriptions_done'
        AND schemaname = 'neon'
    )
    THEN
        CREATE TABLE neon.drop_subscriptions_done
        (id serial primary key, timeline_id text);
    END IF;

    -- preserve the timeline_id of the last drop_subscriptions run
    -- to ensure that the cleanup of a timeline is executed only once.
    -- use upsert to avoid the table bloat in case of cascade branching (branch of a branch)
    INSERT INTO neon.drop_subscriptions_done VALUES (1, current_setting('neon.timeline_id'))
    ON CONFLICT (id) DO UPDATE
    SET timeline_id = current_setting('neon.timeline_id');
END
$$
