begin;
    create table memo(
        id int primary key,
        contents text
    );

    insert into memo(id, contents)
    values
        (1, 'Foo'),
        (2, 'baR'),
        (3, 'baz');

    savepoint a;

    -- Filter by prefix
    select jsonb_pretty(
        graphql.resolve($$
            {
              memoCollection(filter: {contents: {startsWith: "b"}}) {
                edges {
                  node {
                    contents
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;

    -- Confirm that wildcards don't work in prefix filtering
    select jsonb_pretty(
        graphql.resolve($$
            {
              memoCollection(filter: {contents: {startsWith: "b%"}}) {
                edges {
                  node {
                    contents
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;


    -- Filter by like
    select jsonb_pretty(
        graphql.resolve($$
            {
              memoCollection(filter: {contents: {like: "%a%"}}) {
                edges {
                  node {
                    contents
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;

    -- like is case sensitive
    select jsonb_pretty(
        graphql.resolve($$
            {
              memoCollection(filter: {contents: {like: "%r"}}) {
                edges {
                  node {
                    contents
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;

    -- ilike is not case sensitive
    select jsonb_pretty(
        graphql.resolve($$
            {
              memoCollection(filter: {contents: {ilike: "%r"}}) {
                edges {
                  node {
                    contents
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;

    -- Filter by regex
    select jsonb_pretty(
        graphql.resolve($$
            {
              memoCollection(filter: {contents: {regex: "^F\\w+$"}}) {
                edges {
                  node {
                    contents
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;

    -- iregex is not case sensitive
    select jsonb_pretty(
        graphql.resolve($$
            {
              memoCollection(filter: {contents: {iregex: "^f\\w+$"}}) {
                edges {
                  node {
                    contents
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;

rollback;
