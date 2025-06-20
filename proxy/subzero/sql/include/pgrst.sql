
-- code to monitor the last schema update
CREATE SCHEMA IF NOT EXISTS pgrst;

ALTER ROLE authenticator SET pgrst.last_schema_updated = now()::text;
-- Create an event trigger function
CREATE OR REPLACE FUNCTION pgrst.pgrst_watch() RETURNS event_trigger
  LANGUAGE sql
  AS $$
  ALTER ROLE authenticator SET pgrst.last_schema_updated = now()::text;
$$;

CREATE OR REPLACE FUNCTION pgrst.last_schema_updated() RETURNS text
  LANGUAGE sql
  AS $$
  SELECT current_setting('pgrst.last_schema_updated', true);
$$;

-- This event trigger will fire after every ddl_command_end event
CREATE EVENT TRIGGER pgrst.pgrst_watch
  ON ddl_command_end
  EXECUTE PROCEDURE pgrst.pgrst_watch();