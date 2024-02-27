begin;

    comment on schema public is '@graphql({"inflect_names": true})';

    create table public.account(
      id serial primary key,
      first_name varchar(255) not null
    );

    insert into public.account(first_name) values ('foo');

    -- Extend with function
    create function public._raise_err(rec public.account)
      returns text
      immutable
      strict
      language sql
    as $$
      select 1/0 -- divide by 0 error
    $$;

    select
      jsonb_pretty(
        graphql.resolve($$
          {
            accountCollection {
              edges {
                node {
                  id
                  firstName
                  raiseErr
                }
              }
            }
          }
        $$)
      );

    select * from public.account;
rollback;
