begin;

    create role api;
    grant usage on schema graphql to api;
    grant execute on function graphql.resolve to api;

    create table xyz( id int primary key);

    -- Remove mutations so mutationType is null
    revoke update on xyz from api;
    revoke delete on xyz from api;

    set role api;

    -- mutationType should be null
    select jsonb_pretty(
        graphql.resolve($$
            query IntrospectionQuery {
              __schema {
                queryType {
                  name
                }
                mutationType {
                  name
                }
              }
            }
        $$)
    );

rollback;
