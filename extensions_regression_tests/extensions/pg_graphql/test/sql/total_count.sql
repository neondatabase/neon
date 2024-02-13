begin;

    create table account(
        id serial primary key,
        email varchar(255) not null
    );


    insert into public.account(email)
    values
        ('a@x.com'),
        ('b@x.com');


    -- Should fail. totalCount not enabled
    select graphql.resolve($$
    {
      accountCollection {
        totalCount
        edges {
            cursor
        }
      }
    }
    $$);

    -- Enable totalCount
    comment on table account is e'@graphql({"totalCount": {"enabled": true}})';

    -- Should work. totalCount is enabled
    select graphql.resolve($$
    {
      accountCollection {
        totalCount
        edges {
            cursor
        }
      }
    }
    $$);

rollback;
