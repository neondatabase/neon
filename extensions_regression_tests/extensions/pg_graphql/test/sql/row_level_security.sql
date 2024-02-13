begin;
    -- Test that row level security policies are applied for non-superusers
    create role anon;
    alter default privileges in schema public grant all on tables to anon;
    grant usage on schema public to anon;
    grant usage on schema graphql to anon;
    grant all on function graphql.resolve to anon;

    create table account(
        id int primary key
    );

    -- create policy such that only id=2 is visible to anon role
    create policy acct_select
        on public.account
        as permissive
        for select
        to anon
        using (id = 2);

    alter table public.account enable row level security;

    -- Create records fo id 1..10
    insert into public.account(id)
    select * from generate_series(1, 10);

    set role anon;

    -- Only id=2 should be returned
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

rollback;
