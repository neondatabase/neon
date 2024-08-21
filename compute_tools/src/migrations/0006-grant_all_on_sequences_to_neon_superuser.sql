-- SKIP: Deemed insufficient for allowing relations created by extensions to be
--       interacted with by neon_superuser without permission issues.

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO neon_superuser;
