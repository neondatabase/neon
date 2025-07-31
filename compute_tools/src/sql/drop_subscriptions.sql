DO ${outer_tag}$
DECLARE
    subname TEXT;
BEGIN
    LOCK TABLE pg_catalog.pg_subscription IN ACCESS EXCLUSIVE MODE;
    FOR subname IN
        SELECT pg_subscription.subname
        FROM pg_catalog.pg_subscription
        WHERE subdbid OPERATOR(pg_catalog.=) (
            SELECT oid FROM pg_database WHERE datname OPERATOR(pg_catalog.=) {datname_str}::pg_catalog.name
        )
    LOOP
        EXECUTE pg_catalog.format('ALTER SUBSCRIPTION %I DISABLE;', subname);
        EXECUTE pg_catalog.format('ALTER SUBSCRIPTION %I SET (slot_name = NONE);', subname);
        EXECUTE pg_catalog.format('DROP SUBSCRIPTION %I;', subname);
    END LOOP;
END;
${outer_tag}$;
