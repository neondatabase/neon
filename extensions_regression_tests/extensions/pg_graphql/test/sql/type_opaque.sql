begin;
    create table public.device(
        id serial primary key,
        val inet
    );

    -- should work
    select graphql.resolve($$
    mutation {
      insertIntoDeviceCollection(objects: [
        { val: "102.118.1.1" }
      ]) {
        records {
          id
          val
        }
      }
    }
    $$);

    select graphql.resolve($$
    mutation {
      updateDeviceCollection(
        set: {
          val: "1.1.1.1"
        }
        atMost: 1
      ) {
        records {
          id
          val
        }
      }
    }
    $$);

    -- Filter: should work
    select jsonb_pretty(
        graphql.resolve($$
            {
              deviceCollection(filter: {val: {eq: "1.1.1.1"}}) {
                edges {
                  node {
                    id
                    val
                  }
                }
              }
            }
        $$)
    );

    -- Filter: should work
    select jsonb_pretty(
        graphql.resolve($$
            {
              deviceCollection(filter: {val: {is: NOT_NULL}}) {
                edges {
                  node {
                    id
                    val
                  }
                }
              }
            }
        $$)
    );

rollback;
