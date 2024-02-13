begin;
    create table memo(
        id serial primary key,
        vc8 varchar(8),
        c2 char(2)
    );

    insert into memo(vc8, c2)
    values ('foo bar', 'aa');

    -- Expect success
    select graphql.resolve($$
    mutation {
      insertIntoMemoCollection(objects: [
        { vc8: "baz", c2: "bb" }
      ]) {
        records {
          id
          vc8
          c2
        }
      }
    }
    $$);

    -- Expect fail, vc8 too long
    select graphql.resolve($$
    mutation {
      insertIntoMemoCollection(objects: [
        { vc8: "123456789", c2: "bb" }
      ]) {
        records {
          id
          vc8
          c2
        }
      }
    }
    $$);

    -- Expect fail, c2 too long
    select graphql.resolve($$
    mutation {
      insertIntoMemoCollection(objects: [
        { vc8: "12345", c2: "123" }
      ]) {
        records {
          id
          vc8
          c2
        }
      }
    }
    $$);

    -- Expect fail, filter value too long
    select graphql.resolve($$
    {
      memoCollection(filter: {c2: {eq: "too long"}}){
        edges { node { id } }

      }
    }
    $$);

    -- Expect success
    select graphql.resolve($$
    {
      memoCollection(filter: {c2: {eq: "aa"}}){
        edges { node { id } }

      }
    }
    $$);


rollback;
