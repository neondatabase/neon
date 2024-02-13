begin;
    create table account (
        id int primary key
    );

    create function _full_name(rec public.account)
        returns text
        immutable
        strict
        language sql
    as $$
        select 'Foo';
    $$;

    -- Inflection off, Overrides: off
    comment on schema public is e'@graphql({"inflect_names": false})';
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

    savepoint a;

    -- Inflection off, Overrides: on
    comment on function public._full_name(public.account) is E'@graphql({"name": "wholeName"})';
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
    comment on function public._full_name(public.account) is E'@graphql({"name": "WholeName"})';
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
