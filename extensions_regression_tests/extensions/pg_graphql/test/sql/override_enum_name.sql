begin;
    create type account_priority as enum ('high', 'standard');
    comment on type public.account_priority is E'@graphql({"name": "CustomerValue"})';

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "CustomerValue") {
            enumValues {
              name
            }
          }
        }
        $$)
    );

rollback;
