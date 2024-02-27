begin;

    create table account(
        id serial primary key,
        encrypted_password varchar(255) not null,
        parent_id int references account(id)
    );

    insert into public.account(encrypted_password, parent_id)
    values
        ('hidden_hash', 1);

    -- Superuser
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection {
                edges {
                  node {
                    parent {
                      encryptedPassword
                    }
                  }
                }
              }
            }
        $$)
    );

    create role api;

    -- Grant access to GQL
    grant usage on schema graphql to api;

    -- Allow access to public.account.id but nothing else
    grant usage on schema public to api;
    grant all on all tables in schema public to api;
    revoke select on public.account from api;

    grant select (id, parent_id) on public.account to api;

    set role api;

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Account") {
            kind
            fields {
                name
            }
          }
        }
        $$)
    );



    -- Select permitted columns
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection {
                edges {
                  node {
                    parent {
                      id
                    }
                  }
                }
              }
            }
        $$)
    );


    -- Attempt select on revoked column
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection {
                edges {
                  node {
                    parent {
                      encryptedPassword
                    }
                  }
                }
              }
            }
        $$)
    );
rollback;
