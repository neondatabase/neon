begin;
    create table account(
        id int primary key,
        is_verified bool
    );

    insert into account(id) select generate_series(1, 10);

    -- Forward pagination
    -- hasPreviousPage is false, hasNextPage is true
    select jsonb_pretty(
        graphql.resolve($$
        {
          accountCollection(first: 3) {
            pageInfo {
              hasNextPage
              hasPreviousPage
              startCursor
              endCursor
            }
            edges {
              cursor
              node {
                id
              }
            }
          }
        }
        $$)
    );

    -- hasPreviousPage is true, hasNextPage is true
    select jsonb_pretty(
        graphql.resolve($$
        {
          accountCollection(first: 3, after: "WzNd" ) {
            pageInfo {
              hasNextPage
              hasPreviousPage
              startCursor
              endCursor
            }
            edges {
              cursor
              node {
                id
              }
            }
          }
        }
        $$)
    );

    -- hasPreviousPage is false, hasNextPage is true
    select jsonb_pretty(
        graphql.resolve($$
        {
          accountCollection(last: 3, before: "WzRd" ) {
            pageInfo {
              hasNextPage
              hasPreviousPage
              startCursor
              endCursor
            }
            edges {
              cursor
              node {
                id
              }
            }
          }
        }
        $$)
    );


    -- hasPreviousPage is true, hasNextPage is true
    select jsonb_pretty(
        graphql.resolve($$
        {
          accountCollection(last: 2, before: "WzRd" ) {
            pageInfo {
              hasNextPage
              hasPreviousPage
              startCursor
              endCursor
            }
            edges {
              cursor
              node {
                id
              }
            }
          }
        }
        $$)
    );

rollback;
