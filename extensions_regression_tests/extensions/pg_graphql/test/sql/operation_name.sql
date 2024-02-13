begin;

    create table account(
        id serial primary key,
        email varchar(255) not null
    );

    -- Execute first op
    select jsonb_pretty(
        graphql.resolve(
            query := $$
                mutation insertAccount {
                  insertIntoAccountCollection(
                    objects: [
                      {
                        email: "adsf"
                      }
                    ]) {
                    records {
                      id
                     }
                  }
                }

                mutation deleteAccount {
                  deleteFromAccountCollection(
                    filter: {id: {eq: 10}}) {
                    affectedCount
                    records {
                      id
                    }
                  }
                }
            $$,
            "operationName" := 'insertAccount'
        )
    );

    -- Execute second op
    select jsonb_pretty(
        graphql.resolve(
            query := $$
                mutation insertAccount {
                  insertIntoAccountCollection(
                    objects: [
                      {
                        email: "adsf"
                      }
                    ]) {
                    records {
                      id
                     }
                  }
                }

                mutation deleteAccount {
                  deleteFromAccountCollection(
                    filter: {id: {eq: 1}}) {
                    affectedCount
                    records {
                      id
                    }
                  }
                }
            $$,
            "operationName" := 'deleteAccount'
        )
    );

    -- operationName not required for query with 1 operation
    select jsonb_pretty(
        graphql.resolve(
            query := $$
                mutation insertAccount {
                  insertIntoAccountCollection(
                    objects: [
                      {
                        email: "adsf"
                      }
                    ]) {
                    records {
                      id
                     }
                  }
                }
            $$
        )
    );

    -- Fail to pass an operation name
    select jsonb_pretty(
        graphql.resolve(
            query := $$
                mutation insertAccount {
                  insertIntoAccountCollection(
                    objects: [
                      {
                        email: "adsf"
                      }
                    ]) {
                    records {
                      id
                     }
                  }
                }

                mutation deleteAccount {
                  deleteFromAccountCollection(
                    filter: {id: {eq: 10}}) {
                    affectedCount
                    records {
                      id
                    }
                  }
                }
            $$
        )
    );


    -- Pass invalid operation name
    select jsonb_pretty(
        graphql.resolve(
            "operationName" := 'invalidName',
            query := $$
                mutation insertAccount {
                  insertIntoAccountCollection(
                    objects: [
                      {
                        email: "adsf"
                      }
                    ]) {
                    records {
                      id
                     }
                  }
                }

                mutation deleteAccount {
                  deleteFromAccountCollection(
                    filter: {id: {eq: 10}}) {
                    affectedCount
                    records {
                      id
                    }
                  }
                }
            $$
        )
    );

rollback;
