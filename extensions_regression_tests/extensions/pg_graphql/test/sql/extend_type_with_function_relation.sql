begin;
    create table account(
        id serial primary key,
        email varchar(255) not null
    );

    insert into account(email) values ('foo'), ('bar'), ('baz');

    create table blog(
        id serial primary key,
        name varchar(255) not null
    );

    insert into blog(name)
    select
        'blog ' || x
    from
        generate_series(1, 5) y(x);


    create function public.many_blogs(public.account)
        returns setof public.blog
        language sql
        as
    $$
        select * from public.blog where id between $1.id * 4 - 4 and $1.id * 4;
    $$;

    -- To Many

    select jsonb_pretty(
        graphql.resolve($$

        fragment TypeRef on __Type {
          kind
          name
          ofType {
            kind
            name
          }
        }

        {
          __type(name: "Account") {
            fields {
              name
              type {
                ...TypeRef
              }
            }
          }
        }
        $$)
    );

    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection {
                edges {
                  node {
                    id
                    manyBlogs(first: 2) {
                      pageInfo {
                        hasNextPage
                      }
                      edges {
                        node {
                          id
                          name
                        }
                      }
                    }
                  }
                }
              }
            }
        $$)
    );

    -- To One (function returns single value)
    savepoint a;

    create function public.one_account(public.blog)
        returns public.account
        language sql
        as
    $$
        select * from public.account where id = $1.id - 2;
    $$;

    select jsonb_pretty(
        graphql.resolve($$

        fragment TypeRef on __Type {
          kind
          name
          ofType {
            kind
            name
          }
        }

        {
          __type(name: "Blog") {
            fields {
              name
              type {
                ...TypeRef
              }
            }
          }
        }
        $$)
    );


    select jsonb_pretty(
        graphql.resolve($$
            {
              blogCollection(first: 3) {
                edges {
                  node {
                    id
                    oneAccount {
                      id
                      email
                    }
                  }
                }
              }
            }
        $$)
    );

    rollback to savepoint a;

    -- To One (function returns set of <> rows 1)
    create or replace function public.one_account(public.blog)
        returns setof public.account rows 1
        language sql
        as
    $$
        select * from public.account where id = $1.id - 2;
    $$;

    select jsonb_pretty(
        graphql.resolve($$

        fragment TypeRef on __Type {
          kind
          name
          ofType {
            kind
            name
          }
        }

        {
          __type(name: "Blog") {
            fields {
              name
              type {
                ...TypeRef
              }
            }
          }
        }
        $$)
    );


    select jsonb_pretty(
        graphql.resolve($$
            {
              blogCollection(first: 3) {
                edges {
                  node {
                    id
                    oneAccount {
                      id
                      email
                    }
                  }
                }
              }
            }
        $$)
    );

    -- Confirm name overrides work
    comment on function public.one_account(public.blog) is E'@graphql({"name": "acctOverride"})';

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Blog") {
            fields {
              name
            }
          }
        }
        $$)
    );


rollback;
