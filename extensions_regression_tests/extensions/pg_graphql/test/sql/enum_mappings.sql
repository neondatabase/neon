begin;

    create type my_enum as enum ('test', 'valid value', 'another value');

    comment on type my_enum is E'@graphql({"mappings": {"valid value": "valid_value", "another value": "another_value"}})';

    create table enums (
       id serial primary key,
       value my_enum
    );

    -- Seed with value that's valid in both Postgres and GraphQL
    insert into enums (value) values ('test');

    -- Mutation to insert
    select graphql.resolve($$
    mutation {
      insertIntoEnumsCollection(objects: [ { value: "valid_value" } ]) {
          affectedCount
      }
    }
    $$);

    -- Mutation to update
    select graphql.resolve($$
    mutation {
      updateEnumsCollection(set: { value: "another_value" }, filter: { value: {eq: "test"} } ) {
        records { value }
      }
    }
    $$);

    --- Query
    select graphql.resolve($$
        {
          enumsCollection {
            edges {
                node {
                 value
                }
            }
          }
        }
    $$);

    --- Query with filter
    select graphql.resolve($$
        {
          enumsCollection(filter: {value: {eq: "another_value"}}) {
            edges {
                node {
                 value
                }
            }
          }
        }
    $$);

    --- Query with `in` filter
    select graphql.resolve($$
        {
          enumsCollection(filter: {value: {in: ["another_value"]}}) {
            edges {
                node {
                 value
                }
            }
          }
        }
    $$);

    -- Display type via introspection
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "MyEnum") {
            kind
            name
            enumValues {
              name
            }
          }
        }
        $$)
    );

rollback;
