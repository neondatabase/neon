begin;
    create table account(
        id serial primary key,
        email varchar(255) not null
    );

    comment on column public.account.email is E'@graphql({"name": "emailAddress"})';


    -- expect: 'emailAddresses'
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Account") {
            fields {
              name
            }
          }
        }
        $$)
    );

rollback;
