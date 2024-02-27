begin;
    create table account(
        id int primary key
    );

    insert into public.account(id)
    select * from generate_series(1, 100);

    -- expect default 30 rows on first page
    select graphql.resolve($$
      {
        accountCollection
        {
          edges {
            node {
              id
            }
          }
        }
      }
    $$);

    comment on schema public is e'@graphql({"max_rows": 5})';

    -- expect 5 rows on first page
    select graphql.resolve($$
      {
        accountCollection
        {
          edges {
            node {
              id
            }
          }
        }
      }
    $$);

    comment on schema public is e'@graphql({"max_rows": 40})';

    -- expect 40 rows on first page
    select graphql.resolve($$
      {
        accountCollection
        {
          edges {
            node {
              id
            }
          }
        }
      }
    $$);

rollback;
