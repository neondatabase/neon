begin;

    create table account(
        id serial primary key,
        email varchar(255) not null
    );


    insert into public.account(email)
    values
        ('aardvark@x.com'),
        ('bat@x.com'),
        ('cat@x.com'),
        ('dog@x.com'),
        ('elephant@x.com');


    create table blog(
        id serial primary key,
        owner_id integer not null references account(id)
    );


    insert into blog(owner_id)
    values
        ((select id from account where email ilike 'a%')),
        ((select id from account where email ilike 'a%')),
        ((select id from account where email ilike 'a%')),
        ((select id from account where email ilike 'b%'));


    select jsonb_pretty(
        graphql.resolve($$
        {
          blogCollection {
            edges {
              node {
                ownerId
                owner {
                  id
                }
              }
            }
          }
        }
        $$)
    );

rollback;
