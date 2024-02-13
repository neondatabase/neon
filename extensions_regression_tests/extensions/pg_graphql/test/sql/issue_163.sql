begin;
    create table profiles(
        id int primary key,
        username text
    );

    insert into public.profiles(id, username)
    values
        (1, 'foo');

    select jsonb_pretty(
        graphql.resolve($$
            query MyQuery {
              __typename
              profilesCollection {
                edges {
                  node {
                    id
                    username
                  }
                }
              }
            }
        $$)
    )

rollback;
