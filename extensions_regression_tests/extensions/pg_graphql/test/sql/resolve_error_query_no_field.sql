begin;

    select graphql.resolve($$
    {
      account {
        id
      }
    }
    $$);

rollback;
