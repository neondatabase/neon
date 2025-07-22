-- SKIP: Deemed insufficient for allowing relations created by extensions to be
--       interacted with by {privileged_role_name} without permission issues.

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {privileged_role_name};
