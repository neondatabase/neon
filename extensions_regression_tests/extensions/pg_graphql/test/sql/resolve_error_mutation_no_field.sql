begin;

    select graphql.resolve($$
    mutation {
       insertDNE(object: {
        email: "o@r.com"
      }) {
        id
      }
    }
    $$);

rollback;
