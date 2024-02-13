begin;

    create table account(
        id serial primary key,
        email varchar(255) not null,
        priority int
    );

    insert into account(email)
    values ('email_1'), ('email_2');

    -- Scenario: Two queries
    select jsonb_pretty(
        graphql.resolve($$
            {
              forward: accountCollection(orderBy: [{id: AscNullsFirst}]) {
                edges {
                  node {
                    id
                  }
                }
              }
              backward: accountCollection(orderBy: [{id: DescNullsFirst}]) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

rollback
