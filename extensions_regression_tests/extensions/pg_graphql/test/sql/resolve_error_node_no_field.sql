begin;

    create table account(
        id int primary key,
        parent_id int references account(id)
    );


    select graphql.resolve($$
    {
      accountCollection {
        edges {
          cursor
          node {
            parent {
              dneField
            }
          }
        }
      }
    }
    $$);

rollback;
