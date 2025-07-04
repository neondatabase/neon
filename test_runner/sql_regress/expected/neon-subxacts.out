DO $$
DECLARE
i numeric;
BEGIN
  create role somebody;
  FOR i IN 1..1000000 LOOP
    BEGIN
	  IF i % 1000 = 0 THEN
	    alter role somebody password 'welcome';
	  ELSE
        PERFORM 1;
	  END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'error';
    END;
    IF I = 1000000 THEN
      PERFORM pg_log_backend_memory_contexts(pg_backend_pid());
    END IF;
  END LOOP;
END;
$$;
