begin;
    create table account(
        id int primary key
    );


    insert into public.account(id)
    select * from generate_series(1,5);


    select graphql.resolve(
        $$
        query FirstNAccounts($first_: Int!) {
          accountCollection(first: $first_) {
            edges {
              node {
                id
              }
            }
          }
        }
        $$,
        '{"first_": 2}'::jsonb
    );

rollback;
