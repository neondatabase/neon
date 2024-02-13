begin;
    create schema xyz;
    comment on schema xyz is '@graphql({"inflect_names": true})';

    create type xyz.light as enum ('red');

    -- expect nothing b/c not in search_path
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Light") {
            kind
            name
          }
        }
        $$)
    );

    set search_path = 'xyz';

    -- expect 1 record
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Light") {
            kind
            name
          }
        }
        $$)
    );

    revoke all on type xyz.light from public;

    -- Create low priv user without access to xyz.light
    create role low_priv;

    -- expected false
    select pg_catalog.has_type_privilege(
        'low_priv',
        'xyz.light',
        'USAGE'
    );

    grant usage on schema xyz to low_priv;
    grant usage on schema graphql to low_priv;

    set role low_priv;

    -- expect no results b/c low_priv does not have usage permission
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Light") {
            kind
            name
          }
        }
        $$)
    );


rollback;
