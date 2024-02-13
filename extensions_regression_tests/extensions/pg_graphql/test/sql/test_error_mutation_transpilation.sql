begin;
    comment on schema public is '@graphql({"inflect_names": true})';

    create table public.account(
        id serial primary key,
        first_name varchar(255) not null check (first_name not like '%_%')
    );

    -- Second mutation is supposed to generate an exception
    select
      jsonb_pretty(
        graphql.resolve($$
          mutation {
            firstInsert: insertIntoAccountCollection(objects: [
              { firstName: "name" }
            ]) {
              records {
                id
                firstName
              }
            }

            secondInsert: insertIntoAccountCollection(objects: [
              { firstName: "another_name" }
            ]) {
              records {
                id
                firstName
              }
            }
          }
        $$)
      );

    select * from public.account;

rollback;
