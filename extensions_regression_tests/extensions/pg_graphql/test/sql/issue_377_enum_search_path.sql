begin;
    comment on schema public is '@graphql({"inflect_names": false})';

    create schema salt;
    create type salt.encr as enum ('variant');


    create table public.sample(
        id int primary key,
        val salt.encr
    );

    -- encr should not be visible
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "encr") {
            name
          }
        }
        $$)
    );

    -- the `val` column should have opaque type since `encr` not on search path
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "sample") {
            kind
            name
            fields {
              name
              type {
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
        $$)
    );

    -- Adding it to the search path adds `encr` to the schema
    set local search_path = public,salt;

    -- encr now visible
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "encr") {
            kind
            name
            enumValues {
              name
            }
          }
        }
        $$)
    );

    -- A table referencing encr references it vs opaque
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "sample") {
            kind
            name
            fields {
              name
              type {
                name
                kind
              }
            }
          }
        }
        $$)
    );

rollback;
