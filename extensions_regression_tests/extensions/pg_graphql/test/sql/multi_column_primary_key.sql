begin;
    create table person(
        id int primary key,
        name text
    );

    create table address(
        id int primary key,
        city text
    );

    create table person_at_address(
        person_id int not null references person (id),
        address_id int not null references address (id),
        primary key (person_id, address_id)
    );

    insert into public.person(id, name)
    values
        (1, 'foo'),
        (2, 'bar'),
        (3, 'baz');

    insert into public.address(id, city)
    values
        (4, 'Chicago'),
        (5, 'Atlanta'),
        (6, 'Portland');

    insert into public.person_at_address(person_id, address_id)
    values
        (1, 4),
        (2, 4),
        (3, 6);

    savepoint a;

    select jsonb_pretty(
        graphql.resolve($$
            {
              personAtAddressCollection {
                edges {
                  cursor
                  node {
                    nodeId
                    personId
                    addressId
                    person {
                      name
                    }
                    address {
                      city
                    }
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;

    select jsonb_pretty(
        graphql.resolve($$
            {
              personAtAddressCollection(
                first: 1,
                after: "WzEsIDRd"
            ) {
                edges {
                  node {
                    personId
                    addressId
                    nodeId
                  }
                }
              }
            }
        $$)
    );
    rollback to savepoint a;


    select jsonb_pretty(
        graphql.resolve($$
            {
              node(nodeId: "WyJwdWJsaWMiLCAicGVyc29uX2F0X2FkZHJlc3MiLCAxLCA0XQ==") {
                nodeId
                ... on PersonAtAddress {
                  nodeId
                  personId
                  person {
                    name
                    personAtAddressCollection {
                      edges {
                        node {
                          addressId
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
