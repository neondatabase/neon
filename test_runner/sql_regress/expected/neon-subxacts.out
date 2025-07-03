DO $$
DECLARE
i numeric;
BEGIN
  FOR i IN 1..1_000_000 LOOP
    BEGIN
      PERFORM 1;
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'error';
    END;
    IF I = 1_000_000 THEN
      PERFORM pg_log_backend_memory_contexts(pg_backend_pid());
    END IF;
  END LOOP;
END;
$$;
