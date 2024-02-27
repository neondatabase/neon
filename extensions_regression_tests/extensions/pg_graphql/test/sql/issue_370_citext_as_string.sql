begin;
    -- https://github.com/supabase/pg_graphql/issues/370
    -- citext is common enough that we should handle treating it as a string
    create extension citext;

    create table account(
        id int primary key,
        email citext
    );

    insert into public.account(id, email)
    values (1, 'aBc'), (2, 'def');

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Account") {
            kind
            fields {
                name type { kind name ofType { name }  }
            }
          }
        }
        $$)
    );

    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection( filter: {email: {in: ["abc"]}}) {
                edges {
                  node {
                    id
                    email
                  }
                }
              }
            }
        $$)
    );


rollback;
