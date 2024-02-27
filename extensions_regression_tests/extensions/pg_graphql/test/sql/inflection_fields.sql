begin;
    create table account (
        id int primary key,
        name_with_underscore text
    );

    -- Inflection off, Overrides: off
    comment on schema public is e'@graphql({"inflect_names": false})';
    savepoint a;

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "account") {
            fields {
                name
            }
          }
        }
        $$)
    );

    -- Inflection off, Overrides: on
    comment on column account.id is e'@graphql({"name": "IddD"})';
    comment on column account.name_with_underscore is e'@graphql({"name": "nAMe"})';
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "account") {
            fields {
                name
            }
          }
        }
        $$)
    );

    rollback to savepoint a;

    -- Inflection on, Overrides: off
    comment on schema public is e'@graphql({"inflect_names": true})';
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Account") {
            fields {
                name
            }
          }
        }
        $$)
    );

    -- Inflection on, Overrides: on
    comment on column account.id is e'@graphql({"name": "IddD"})';
    comment on column account.name_with_underscore is e'@graphql({"name": "nAMe"})';
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Account") {
            fields {
                name
            }
          }
        }
        $$)
    );

rollback;
