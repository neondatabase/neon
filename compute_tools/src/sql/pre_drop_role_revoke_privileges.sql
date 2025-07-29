DO ${outer_tag}$
DECLARE
    schema TEXT;
    grantor TEXT;
    revoke_query TEXT;
BEGIN
    FOR schema IN
        SELECT schema_name
        FROM information_schema.schemata
        -- So far, we only had issues with 'public' schema. Probably, because we do some additional grants,
        -- e.g., make DB owner the owner of 'public' schema automatically (when created via API).
        -- See https://github.com/neondatabase/cloud/issues/13582 for the context.
        -- Still, keep the loop because i) it efficiently handles the case when there is no 'public' schema,
        -- ii) it's easy to add more schemas to the list if needed.
        WHERE schema_name IN ('public')
    LOOP
        FOR grantor IN EXECUTE
            pg_catalog.format(
                'SELECT DISTINCT rtg.grantor FROM information_schema.role_table_grants AS rtg WHERE grantee OPERATOR(pg_catalog.=) %s',
                -- N.B. this has to be properly dollar-escaped with `pg_quote_dollar()`
                quote_literal({role_name})
            )
        LOOP
            EXECUTE pg_catalog.format('SET LOCAL ROLE %I', grantor);

            revoke_query := pg_catalog.format(
                'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM %I GRANTED BY %I',
                schema,
                -- N.B. this has to be properly dollar-escaped with `pg_quote_dollar()`
                {role_name},
                grantor
            );

            EXECUTE revoke_query;
        END LOOP;
    END LOOP;
END;
${outer_tag}$;
