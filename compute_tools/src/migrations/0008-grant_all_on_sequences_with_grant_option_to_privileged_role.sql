-- SKIP: Moved inline to the handle_grants() functions.

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {privileged_role_name} WITH GRANT OPTION;
