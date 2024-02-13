begin;

    create table account(
        id int primary key
    );


    select graphql.resolve($$
    {
      accountCollection {
        edges {
          cursor
          node {
            dneField
          }
        }
      }
    }
    $$);

rollback;
