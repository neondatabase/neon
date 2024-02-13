select
    jsonb_build_object(
        'search_path', current_schemas(false),
        'role', current_role,
        'schema_version', graphql.get_schema_version()
    )
