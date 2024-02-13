begin;

    /*
        Composite and array types are not currently supported as inputs
        - confirm composites are not allowed anywhere
        - confirm arrays are not allowed as input
    */

    create type complex as (r int, i int);

    create table something(
        id serial primary key,
        name varchar(255) not null,
        tags text[],
        comps complex,
        js json,
        jsb jsonb
    );

    -- Inflection on, Overrides: off
    comment on schema public is e'@graphql({"inflect_names": true})';
    select jsonb_pretty(
        jsonb_path_query(
            graphql.resolve($$
                {
                  __schema {
                    types {
                      name
                      fields {
                        name
                      }
                      inputFields {
                        name
                      }
                    }
                  }
                }
            $$),
            '$.data.__schema.types[*] ? (@.name starts with "Something")'
        )
    );

rollback;
