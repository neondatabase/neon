begin;

    create table account(
        id int primary key
    );


    select graphql.resolve($$
    {
      accountCollection {
        totalCount
        edges {
            dneField
        }
      }
    }
    $$);

rollback;
