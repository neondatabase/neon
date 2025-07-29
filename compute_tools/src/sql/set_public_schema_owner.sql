DO ${outer_tag}$
    DECLARE
        schema_owner TEXT;
    BEGIN
        IF EXISTS(
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname OPERATOR(pg_catalog.=) 'public'::pg_catalog.name
        )
        THEN
            SELECT nspowner::regrole::text
            FROM pg_catalog.pg_namespace
            WHERE nspname OPERATOR(pg_catalog.=) 'public'::pg_catalog.text
            INTO schema_owner;

            IF schema_owner OPERATOR(pg_catalog.=) 'cloud_admin'::pg_catalog.text OR schema_owner OPERATOR(pg_catalog.=) 'zenith_admin'::pg_catalog.text
            THEN
                EXECUTE pg_catalog.format('ALTER SCHEMA public OWNER TO %I', {db_owner});
            END IF;
        END IF;
    END
${outer_tag}$;