DO ${outer_tag}$
    BEGIN
        IF EXISTS(
            SELECT 1
            FROM pg_catalog.pg_database
            WHERE datname = {datname}
        )
        THEN
            EXECUTE pg_catalog.format('ALTER DATABASE %I is_template false', {datname});
        END IF;
    END
${outer_tag}$;
