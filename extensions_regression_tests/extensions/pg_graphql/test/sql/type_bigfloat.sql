begin;
    create table public.amount(
        id serial primary key,
        val numeric(10,2)
    );

    insert into public.amount(val)
    values
        ('123.45'),
        ('543.21');

    -- should work
    select graphql.resolve($$
    mutation {
      insertIntoAmountCollection(objects: [
        { val: "123.45" }
      ]) {
        records {
          id
          val
        }
      }
    }
    $$);

    savepoint a;

    -- should fail: must be a string
    select graphql.resolve($$
    mutation {
      insertIntoAmountCollection(objects: [
        { val: 543.25 }
      ]) {
        records {
          id
          val
        }
      }
    }
    $$);

    rollback to savepoint a;

    select graphql.resolve($$
    mutation {
      updateAmountCollection(
        set: {
          val: "222.65"
        }
        filter: {id: {eq: 1}}
        atMost: 1
      ) {
        records { id }
      }
    }
    $$);

    -- Filter: should work
    select jsonb_pretty(
        graphql.resolve($$
            {
              amountCollection(filter: {val: {eq: "222.65"}}) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );


    -- should fail: must be string
    select jsonb_pretty(
        graphql.resolve($$
            {
              amountCollection(filter: {val: {lt: 9999}}) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );



rollback;
