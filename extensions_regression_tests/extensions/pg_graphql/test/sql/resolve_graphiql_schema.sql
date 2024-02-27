begin;
    create extension citext;

    create table account(
        id serial primary key,
        email citext not null,
        encrypted_password varchar(255) not null,
        created_at timestamp not null,
        updated_at timestamp not null
    );
    comment on table account is e'@graphql({"totalCount": {"enabled": true}})';

    create view person as
        select * from account;

    comment on view person is e'
    @graphql({
        "primary_key_columns": ["id"]
    })';

    create table blog(
        id serial primary key,
        owner_id integer not null references account(id),
        name varchar(255) not null,
        description varchar(255),
        tags text[] not null,
        created_at timestamp not null,
        updated_at timestamp not null
    );

    comment on table blog is e'
    @graphql({
        "foreign_keys": [
          {
            "local_name": "blogs",
            "local_columns": ["owner_id"],
            "foreign_name": "personAuthor",
            "foreign_schema": "public",
            "foreign_table": "person",
            "foreign_columns": ["id"]
          }
        ]
    })';

    create type blog_post_status as enum ('PENDING', 'RELEASED');


    create table blog_post(
        id uuid not null default gen_random_uuid() primary key,
        blog_id integer not null references blog(id),
        title varchar(255) not null,
        body varchar(10000),
        status blog_post_status not null,
        created_at timestamp not null,
        updated_at timestamp not null
    );


    select jsonb_pretty(
        graphql.resolve($$


    query IntrospectionQuery {
      __schema {

        queryType {
          name
        }
        mutationType {
          name
        }
        subscriptionType {
          name
        }
        types {
          ...FullType
        }
        directives {
          name
          description
          isRepeatable
          locations
          args {
            ...InputValue
          }
          __typename
        }
      }
    }

    fragment FullType on __Type {
      kind
      name
      description
      fields(includeDeprecated: true) {
        name
        description
        args {
          ...InputValue
        }
        type {
          ...TypeRef
        }
        isDeprecated
        deprecationReason
      }
      inputFields {
        ...InputValue
      }
      interfaces {
        ...TypeRef
      }
      enumValues(includeDeprecated: true) {
        name
        description
        isDeprecated
        deprecationReason
      }
      possibleTypes {
        ...TypeRef
      }
    }

    fragment InputValue on __InputValue {
      name
      description
      type {
        ...TypeRef
      }
      defaultValue
    }

    fragment TypeRef on __Type {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
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

rollback;
