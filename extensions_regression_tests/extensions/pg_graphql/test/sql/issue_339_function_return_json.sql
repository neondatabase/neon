begin;
    create table public.account(
        id int primary key
    );

    create function public._computed(rec public.account)
        returns json
        immutable
        strict
        language sql
    as $$
        select jsonb_build_object('hello', 'world');
    $$;

    insert into account(id) values (1);

    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection {
                edges {
                  node {
                    id
                    computed
                  }
                }
              }
            }
        $$)
    );

rollback;
