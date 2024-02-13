begin;

    create type sub_status as enum ('invited', 'not_invited');
    alter type sub_status add value if not exists 'opened' after 'invited';

    create table account(
        id int primary key,
        ss sub_status
    );

    insert into public.account(id)
    select * from generate_series(1,5);

    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(first: 1) {
                edges {
                  node {
                    id
                    ss
                  }
                }
              }
            }
        $$)
    );

rollback;
