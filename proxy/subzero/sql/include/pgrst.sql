
-- code to monitor the last schema update
CREATE SCHEMA IF NOT EXISTS pgrst;

ALTER ROLE authenticator SET pgrst.last_schema_updated = '';
-- Create an event trigger function
CREATE OR REPLACE FUNCTION pgrst.pgrst_watch() RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
  current_timestamp_text TEXT;
BEGIN
  current_timestamp_text := now()::text;
  EXECUTE 'ALTER ROLE authenticator SET pgrst.last_schema_updated = ' || quote_literal(current_timestamp_text);
END;
$$;


CREATE OR REPLACE FUNCTION pgrst.last_schema_updated() RETURNS text
  LANGUAGE sql
  AS $$
  SELECT current_setting('pgrst.last_schema_updated', true);
$$;

-- This event trigger will fire after every ddl_command_end event
CREATE EVENT TRIGGER pgrst_watch
  ON ddl_command_end
  EXECUTE PROCEDURE pgrst.pgrst_watch();