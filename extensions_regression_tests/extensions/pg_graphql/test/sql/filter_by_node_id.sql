begin;
    create table account(
        id int primary key,
        email text
    );

    insert into public.account(id, email)
    values
        (1, 'foo@foo.com'),
        (2, 'bar@bar.com'),
        (3, 'baz@baz.com');

    savepoint a;

    -- Display the node_ids
    select jsonb_pretty(
        graphql.resolve($${accountCollection { edges { node { id nodeId } } }}$$)
    );

    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(filter: { nodeId: { eq: "WyJwdWJsaWMiLCAiYWNjb3VudCIsIDJd"} } ) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

    -- Select by nodeId
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(
                filter: {
                  nodeId: {eq: "WyJwdWJsaWMiLCAiYWNjb3VudCIsIDJd"}
                }
              ) {
                edges {
                  node {
                    id
                    nodeId
                  }
                }
              }
            }$$
        )
    );

    -- Update by nodeId
    select graphql.resolve($$
    mutation {
      updateAccountCollection(
        set: {
          email: "new@email.com"
        }
        filter: {
          nodeId: {eq: "WyJwdWJsaWMiLCAiYWNjb3VudCIsIDJd"}
        }
      ) {
        records { id }
      }
    }
    $$);
    rollback to savepoint a;

    -- Delete by nodeId
    select graphql.resolve($$
    mutation {
      deleteFromAccountCollection(
        filter: {
          nodeId: {eq: "WyJwdWJsaWMiLCAiYWNjb3VudCIsIDJd"}
        }
      ) {
        records { id }
      }
    }
    $$);
    select * from public.account;
    rollback to savepoint a;

    -- ERRORS: use incorrect table
    select graphql.encode('["public", "blog", 1]'::jsonb);

    -- Wrong table
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(
                filter: {
                  nodeId: {eq: "WyJwdWJsaWMiLCAiYmxvZyIsIDFd"}
                }
              ) {
                edges {
                  node {
                    id
                    nodeId
                  }
                }
              }
            }$$
        )
    );

rollback;
