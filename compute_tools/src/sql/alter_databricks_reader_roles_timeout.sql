DO $$
DECLARE
    reader_role RECORD;
    timeout_value TEXT;
BEGIN
    -- Get the current GUC setting for reader statement timeout
    SELECT current_setting('databricks.reader_statement_timeout', true) INTO timeout_value;
    
    -- Only proceed if timeout_value is not null/empty and not '0' (disabled)
    IF timeout_value IS NOT NULL AND timeout_value != '' AND timeout_value != '0' THEN
        -- Find all databricks_reader_* roles and update their statement_timeout
        FOR reader_role IN 
            SELECT r.rolname
            FROM pg_roles r
            WHERE r.rolname ~ '^databricks_reader_\d+$'
        LOOP
            -- Apply the timeout setting to the role (will overwrite existing setting)
            EXECUTE format('ALTER ROLE %I SET statement_timeout = %L', 
                         reader_role.rolname, timeout_value);
            
            RAISE LOG 'Updated statement_timeout = % for role %', timeout_value, reader_role.rolname;
        END LOOP;
    END IF;
END
$$;
