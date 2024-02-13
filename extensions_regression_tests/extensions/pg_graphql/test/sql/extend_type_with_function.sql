begin;
    comment on schema public is '@graphql({"inflect_names": true})';

    create table public.account(
        id serial primary key,
        first_name varchar(255) not null,
        last_name varchar(255) not null,
        parent_id int references account(id)
    );

    -- Extend with function
    create function public._full_name(rec public.account)
        returns text
        immutable
        strict
        language sql
    as $$
        select format('%s %s', rec.first_name, rec.last_name)
    $$;

    insert into public.account(first_name, last_name, parent_id)
    values
        ('Foo', 'Fooington', 1);


    select jsonb_pretty(
        graphql.resolve($$
    {
      accountCollection {
        edges {
          node {
            id
            firstName
            lastName
            fullName
            parent {
              fullName
            }
          }
        }
      }
    }
        $$)
    );


rollback;
