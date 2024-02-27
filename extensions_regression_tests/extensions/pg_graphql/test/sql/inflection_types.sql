begin;
    create table blog_post(
        id int primary key,
        author_id int
    );

    savepoint a;

    -- Inflection off, Overrides: off
    comment on schema public is e'@graphql({"inflect_names": false})';

    select jsonb_pretty(
        jsonb_path_query(
            graphql.resolve($$
                query IntrospectionQuery {
                  __schema {
                    types {
                      name
                    }
                  }
                }
            $$),
            '$.data.__schema.types[*].name ? (@ starts with "blog")'
        )
    );

    -- Inflection off, Overrides: on
    comment on table blog_post is e'@graphql({"name": "BlogZZZ"})';
    select jsonb_pretty(
        jsonb_path_query(
            graphql.resolve($$
                query IntrospectionQuery {
                  __schema {
                    types {
                      name
                    }
                  }
                }
            $$),
            '$.data.__schema.types[*].name ? (@ starts with "Blog")'
        )
    );

    rollback to savepoint a;

    -- Inflection on, Overrides: off
    comment on schema public is e'@graphql({"inflect_names": true})';
    select jsonb_pretty(
        jsonb_path_query(
            graphql.resolve($$
                query IntrospectionQuery {
                  __schema {
                    types {
                      name
                    }
                  }
                }
            $$),
            '$.data.__schema.types[*].name ? (@ starts with "Blog")'
        )
    );

    -- Inflection on, Overrides: on
    comment on table blog_post is e'@graphql({"name": "BlogZZZ"})';
    select jsonb_pretty(
        jsonb_path_query(
            graphql.resolve($$
                query IntrospectionQuery {
                  __schema {
                    types {
                      name
                    }
                  }
                }
            $$),
            '$.data.__schema.types[*].name ? (@ starts with "Blog")'
        )
    );

rollback;
