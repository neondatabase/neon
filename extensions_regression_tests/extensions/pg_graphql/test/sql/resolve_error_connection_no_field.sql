begin;

    create table account(
        id int primary key
    );


    select graphql.resolve($$
    {
      accountCollection {
        dneField
        totalCount
      }
    }
    $$);

rollback;
