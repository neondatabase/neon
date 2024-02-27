begin;
    create table public.account(
        id int primary key
    );

    create function public._one(rec public.account)
        returns int
        immutable
        strict
        language sql
    as $$
        select 1
    $$;

    comment on table public.account
    is e'@graphql({"description": "Some Description"})';

    comment on column public.account.id
    is e'@graphql({"description": "Some Other Description"})';

    comment on function public._one
    is e'@graphql({"description": "Func Description"})';

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Account") {
            kind
            description
            fields {
              name
              description
            }
          }
        }
        $$)
    );

rollback;
