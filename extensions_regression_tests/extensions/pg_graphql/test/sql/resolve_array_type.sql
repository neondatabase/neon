begin;

    create table xrr(
        id bigserial primary key,
        tags text[],
        nums int[],
        uids uuid[]
    );

    insert into xrr(id, tags, nums)
    values (9, array['a', 'b'], array[1, 2]);

    select jsonb_pretty(
        graphql.resolve($$
            {
              xrrCollection {
                edges {
                  node {
                    id
                    tags
                  }
                }
              }
            }
        $$)
    );

    -- Insert
    select jsonb_pretty(
        graphql.resolve($$
    mutation {
      insertIntoXrrCollection(objects: [
        { nums: 1 },
        { tags: "b", nums: null },
        { tags: ["c", "d"], nums: [3, null] },
      ]) {
        affectedCount
        records {
          id
          tags
          nums
        }
      }
    }
    $$));

    -- Update
    select jsonb_pretty(
        graphql.resolve($$
    mutation {
      updateXrrCollection(
        filter: { id: { gte: "8" } },
        set: { tags: "g" }
      ) {
        affectedCount
        records {
          id
          tags
          nums
        }
      }
    }
    $$));

    -- Delete
    select jsonb_pretty(
        graphql.resolve($$
    mutation {
      updateXrrCollection(
        filter: { id: { eq: 1 } },
        set: { tags: ["h", null, "i"], uids: [null, "9fb1c8e9-da2a-4072-b9fb-4f277446df9c"] }
      ) {
        affectedCount
        records {
          id
          tags
          nums
          uids
        }
      }
    }
    $$));

    select * from xrr;

rollback;
