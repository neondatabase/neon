begin;
    -- Test that the argument parser can handle null values

    create table account(
        id serial primary key,
        email text
    );

    select graphql.resolve($$
    mutation {
      insertIntoAccountCollection(objects: [
        { email: null }
      ]) {
        affectedCount
        records {
          id
          email
        }
      }
    }
    $$);

rollback;
