begin;

    create table account(
        id serial primary key,
        email varchar(255) not null,
        encrypted_password varchar(255) not null,
        created_at timestamp not null,
        updated_at timestamp not null
    );


    create table blog(
        id serial primary key,
        owner_id integer not null references account(id),
        name varchar(255) not null,
        description varchar(255),
        created_at timestamp not null,
        updated_at timestamp not null
    );


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
                types {
                  kind
                  name
                }
                directives {
                  name
                  description
                  locations
                }
              }
            }
        $$)
    );


rollback;
