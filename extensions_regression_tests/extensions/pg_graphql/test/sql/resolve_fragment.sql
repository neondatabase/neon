begin;

    create table blog(
        id serial primary key,
        owner_id integer not null,
        name varchar(255) not null,
        description text
    );

    insert into blog(owner_id, name, description)
    values
        (1, 'A: Blog 1', 'first'),
        (2, 'A: Blog 2', 'second');


    select graphql.resolve($$
        {
          blogCollection(first: 1) {
            edges {
              cursor
              node {
                ...BaseBlog
                ownerId
              }
            }
          }
        }

        fragment BaseBlog on Blog {
          name
          description
        }
    $$);

rollback;
