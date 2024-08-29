-- SKIP: Moved inline to the handle_grants() functions.

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO neon_superuser WITH GRANT OPTION;
