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
        owner_id integer not null references account(id),
        name varchar(255) not null
    );
    comment on table blog is e'@graphql({"totalCount": {"enabled": true}})';


    insert into blog(owner_id, name)
    values
        ((select id from account where email ilike 'a%'), 'A: Blog 1'),
        ((select id from account where email ilike 'a%'), 'A: Blog 2'),
        ((select id from account where email ilike 'a%'), 'A: Blog 3'),
        ((select id from account where email ilike 'b%'), 'B: Blog 4');


    select jsonb_pretty(
        graphql.resolve($$
    {
      accountCollection {
        edges {
          node {
            id
            email
            blogCollection {
              totalCount
                edges {
                  node {
                    name
                }
              }
            }
          }
        }
      }
    }
        $$)
    );


rollback;
