begin;
    create table account(
        id serial primary key,
        email varchar(255) not null
    );

    insert into account(email) values ('foo'), ('bar'), ('baz');

    create view person as
        select * from account;

    create table blog(
        id serial primary key,
        account_id integer not null, -- references account(id)
        name varchar(255) not null
    );

    insert into blog(account_id, name)
    values (1, 'Blog A'), (2, 'Blog B');

    -- No entry for "personCollection" since it has no primary key
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Query") {
            fields {
              name
            }
          }
        }
        $$)
    );

    comment on view person is e'
    @graphql({
        "primary_key_columns": ["id"]
    })';


    -- CRUD

    -- "personCollection" exists because it now has a primary key
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Query") {
            fields {
              name
            }
          }
        }
        $$)
    );

    select jsonb_pretty(
        graphql.resolve($$
        {
          personCollection (first: 2) {
            edges {
              node {
                nodeId
                email
              }
            }
          }
        }
        $$)
    );

    -- "person" is a simple view so it is insertable, updatable, and deletable
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Mutation") {
            fields {
              name
            }
          }
        }
        $$)
    );



    -- insert
    select jsonb_pretty(
        graphql.resolve($$
        mutation {
          insertIntoPersonCollection (
            objects: {email: "quz"}
          ) {
            affectedCount
            records {
              id
              nodeId
              email
            }
          }
        }
        $$)
    );

    -- update
    select jsonb_pretty(
        graphql.resolve($$
        mutation {
          updatePersonCollection (
            set: {email: "thud"}
            filter: {email: {eq: "quz"}}
          ) {
            affectedCount
            records {
              id
              nodeId
              email
            }
          }
        }
        $$)
    );

    -- delete
    select jsonb_pretty(
        graphql.resolve($$
        mutation {
          deleteFromPersonCollection (
            filter: {email: {eq: "thud"}}
          ) {
            affectedCount
            records {
              id
              nodeId
              email
            }
          }
        }
        $$)
    );


    -- Relationships with explicit names

    comment on table blog is e'
    @graphql({
        "foreign_keys": [
          {
            "local_name": "blogs",
            "local_columns": ["account_id"],
            "foreign_name": "person",
            "foreign_schema": "public",
            "foreign_table": "person",
            "foreign_columns": ["id"]
          }
        ]
    })';

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Person") {
            fields {
              name
            }
          }
        }
        $$)
    );

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

    select jsonb_pretty(
        graphql.resolve($$
        {
          personCollection (first: 2) {
            edges {
              node {
                email
                blogs(first: 1) {
                  edges {
                    node {
                      name
                      person {
                        email
                      }
                    }
                  }
                }
              }
            }
          }
        }
        $$)
    );

    -- Relationships with default names (check that inflection rules still work)

    comment on table blog is e'
    @graphql({
        "foreign_keys": [
          {
            "local_columns": ["account_id"],
            "foreign_schema": "public",
            "foreign_table": "person",
            "foreign_columns": ["id"]
          }
        ]
    })';

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "Person") {
            fields {
              name
            }
          }
        }
        $$)
    );

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

    select jsonb_pretty(
        graphql.resolve($$
        {
          personCollection (first: 2) {
            edges {
              node {
                email
                blogCollection(first: 1) {
                  edges {
                    node {
                      name
                      account {
                        email
                      }
                    }
                  }
                }
              }
            }
          }
        }
        $$)
    );


    -- Error states

    -- Invalid structure of comment directive (columns not a list)
    comment on table blog is e'
    @graphql({
        "foreign_keys": [
          {
            "local_columns": "account_id",
            "foreign_schema": "public",
            "foreign_table": "person",
            "foreign_columns": ["id"]
          }
        ]
    })';

    select jsonb_pretty(
        graphql.resolve($$
        {
          personCollection (first: 1) {
            edges {
              node {
                email
              }
            }
          }
        }
        $$)
    );

rollback;
