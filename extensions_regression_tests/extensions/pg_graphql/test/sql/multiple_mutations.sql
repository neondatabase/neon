begin;

    create table account(
        id serial primary key,
        email varchar(255) not null,
        priority int
    );

    savepoint a;

    -- Scenario: Two Mutations, both are vaild
    select jsonb_pretty(graphql.resolve($$
    mutation {
      firstInsert: insertIntoAccountCollection(objects: [
        { email: "foo@barsley.com", priority: 1 }
      ]) {
        affectedCount
        records {
          id
          email
        }
      }

      secondInsert: insertIntoAccountCollection(objects: [
        { email: "bar@foosworth.com" }
      ]) {
        affectedCount
        records {
          id
          email
        }
      }
    }
    $$));

    select * from account;

    rollback to savepoint a;

    -- Scenario: Two Mutations, first one fails. Expect total rollback
    select jsonb_pretty(graphql.resolve($$
    mutation {
      firstInsert: insertIntoAccountCollection(objects: [
        { email: "foo@barsley.com", invalidKey: 1 }
      ]) {
        records {
          id
          email
        }
      }

      secondInsert: insertIntoAccountCollection(objects: [
        { email: "bar@foosworth.com" }
      ]) {
        records {
          id
          email
        }
      }
    }
    $$));

    select * from account;

    rollback to savepoint a;

    -- Scenario: Two Mutations, second one fails. Expect total rollback
    select jsonb_pretty(graphql.resolve($$
    mutation {
      secondInsert: insertIntoAccountCollection(objects: [
        { email: "bar@foosworth.com" }
      ]) {
        records {
          id
          email
        }
      }

    firstInsert: insertIntoAccountCollection(objects: [
        { email: "foo@barsley.com", invalidKey: 1 }
      ]) {
        records {
          id
          email
        }
      }
    }
    $$));

    select * from account;

    rollback to savepoint a;

rollback
