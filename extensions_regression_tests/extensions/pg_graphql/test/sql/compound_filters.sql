begin;

    CREATE TYPE plan AS ENUM ('free', 'pro', 'enterprise');

    create table account(
        id serial primary key,
        email varchar(255) not null,
        plan plan not null
    );

    insert into public.account(email, plan)
    values
        ('aardvark@x.com', 'free'),
        ('bat@x.com', 'pro'),
        ('cat@x.com', 'enterprise'),
        ('dog@x.com', 'free'),
        ('elephant@x.com', 'pro');

    savepoint a;

    -- `and` filter zero expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {and: []}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- `and` filter one expression
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {and: [{id: {eq: 1}}]}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- `and` filter two expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {and: [{id: {eq: 1}}, {email: {eq: "aardvark@x.com"}}]}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- `and` filter three expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {and: [{id: {eq: 1}}, {email: {eq: "aardvark@x.com"}}, {plan: {eq: "free"}}]}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- `or` filter zero expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {or: []}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- `or` filter one expression
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {or: [{id: {eq: 1}}]}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- `or` filter two expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {or: [{id: {eq: 3}}, {email: {eq: "elephant@x.com"}}]}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- `or` filter three expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {or: [{id: {eq: 1}}, {email: {eq: "bat@x.com"}}, {plan: {eq: "enterprise"}}]}) {
                edges {
                    node {
                        id
                        email
                        plan
                    }
                }
            }
        }
        $$)
    );

    -- `not` filter zero expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {not: {}}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- `not` filter one expression
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {not: {id: {eq: 3}}}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- multiple expressions inside a `not` filter are implicitly `and`ed together
    -- `not` filter two expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {not: {id: {eq: 1}, email: {eq: "aardvark@x.com"}}}) {
                edges {
                    node {
                        id
                        email
                    }
                }
            }
        }
        $$)
    );

    -- multiple expressions inside a `not` filter are implicitly `and`ed together
    -- `not` filter three expressions
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(filter: {not: {id: {eq: 1}, email: {eq: "aardvark@x.com"}, plan: {eq: "free"}}}) {
                edges {
                    node {
                        id
                        email
                        plan
                    }
                }
            }
        }
        $$)
    );

    -- `and` filter (explicit) nested inside `or`
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(
                filter: {
                    or: [
                        { id: { eq: 3 } }
                        { id: { eq: 5 } }
                        { and: [{ id: { eq: 1 } }, { email: { eq: "aardvark@x.com" } }] } # explicit and
                    ]
                }
            ) {
                edges {
                node {
                    id
                    email
                }
                }
            }
        }
        $$)
    );

    -- `and` filter (implicit) nested inside `or`
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(
                filter: {
                    or: [
                        { id: { eq: 3 } }
                        { id: { eq: 5 } }
                        { id: { eq: 1 }, email: { eq: "aardvark@x.com" } } # implicit and
                    ]
                }
            ) {
                edges {
                node {
                    id
                    email
                }
                }
            }
        }
        $$)
    );

    -- `or` filter nested inside and
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(
                filter: {
                    and: [
                        { id: { gt: 0 } }
                        { id: { lt: 4 } }
                        { or: [{email: { eq: "bat@x.com" }}, {email: { eq: "cat@x.com" }}] }
                    ]
                }
            ) {
                edges {
                node {
                    id
                    email
                }
                }
            }
        }
        $$)
    );

    -- `not` filter nested inside `or` which is nested inside `and`
    select jsonb_pretty(
        graphql.resolve($$
        {
            accountCollection(
                filter: {
                    and: [
                        { id: { gt: 0 } }
                        { id: { lt: 4 } }
                        { or: [{not: {email: { eq: "bat@x.com" }}}, {email: { eq: "cat@x.com" }}] }
                    ]
                }
            ) {
                edges {
                node {
                    id
                    email
                }
                }
            }
        }
        $$)
    );

    -- update by compound filters
    select graphql.resolve($$
        mutation {
            updateAccountCollection(
                set: {
                    email: "new@email.com"
                }
                filter: {
                    or: [
                        { id: { eq: 3 } }
                        { id: { eq: 5 } }
                        { and: [{ id: { eq: 1 } }, { email: { eq: "aardvark@x.com" } }] }
                    ]
                }
                atMost: 5
            ) {
                records { id, email }
            }
        }
        $$
    );
    rollback to savepoint a;

    -- delete by compound filters
    select graphql.resolve($$
        mutation {
            deleteFromAccountCollection(
                filter: {
                    or: [
                        { id: { eq: 3 } }
                        { id: { eq: 5 } }
                        { id: { eq: 1 }, email: { eq: "aardvark@x.com" } }
                    ]
                }
                atMost: 5
            ) {
                records { id }
            }
        }
        $$
    );
    rollback to savepoint a;

    -- columns named `and`, `or` and `not`, all compound filters will be disabled
    comment on schema public is e'@graphql({"inflect_names": false})';
    create table clashes(
        "and" serial primary key,
        "or" varchar(255) not null,
        "not" plan not null
    );

    insert into public.clashes("or", "not")
    values
        ('aardvark@x.com', 'free'),
        ('bat@x.com', 'pro'),
        ('cat@x.com', 'enterprise'),
        ('dog@x.com', 'free'),
        ('elephant@x.com', 'pro');

    select jsonb_pretty(
        graphql.resolve($$
        {
            clashesCollection(filter: {and: {eq: 1}, or: {eq: "aardvark@x.com"}, not: {eq: "free"}}) {
                edges {
                    node {
                        and
                        or
                        not
                    }
                }
            }
        }
        $$)
    );
    rollback to savepoint a;

    -- column named `and`. `and` compound filter will be disabled, others should work
    comment on schema public is e'@graphql({"inflect_names": false})';
    create table clashes(
        "and" serial primary key,
        email varchar(255) not null,
        plan plan not null
    );

    insert into public.clashes(email, plan)
    values
        ('aardvark@x.com', 'free'),
        ('bat@x.com', 'pro'),
        ('cat@x.com', 'enterprise'),
        ('dog@x.com', 'free'),
        ('elephant@x.com', 'pro');

    select jsonb_pretty(
        graphql.resolve($$
        {
            clashesCollection(
                filter: {
                    or: [
                        { and: { eq: 3 } }
                        { and: { eq: 5 } }
                        { and: { eq: 2 }, not: { email: { eq: "aardvark@x.com" }} }
                    ]
                }
            ) {
                edges {
                    node {
                        and
                        email
                        plan
                    }
                }
            }
        }
        $$)
    );
    rollback to savepoint a;

    -- column named `or`. `or` compound filter will be disabled, others should work
    comment on schema public is e'@graphql({"inflect_names": false})';
    create table clashes(
        id serial primary key,
        "or" varchar(255) not null,
        plan plan not null
    );

    insert into public.clashes("or", plan)
    values
        ('aardvark@x.com', 'free'),
        ('bat@x.com', 'pro'),
        ('cat@x.com', 'enterprise'),
        ('dog@x.com', 'free'),
        ('elephant@x.com', 'pro');

    select jsonb_pretty(
        graphql.resolve($$
        {
            clashesCollection(
                filter: {
                    and: [ {not: {id: { eq: 2 }}}, { or: { neq: "aardvark@x.com" }}]
                }
            ) {
                edges {
                    node {
                        id
                        or
                        plan
                    }
                }
            }
        }
        $$)
    );
    rollback to savepoint a;

    -- column named `not`. `not` compound filter will be disabled, others should work
    comment on schema public is e'@graphql({"inflect_names": false})';
    create table clashes(
        id serial primary key,
        email varchar(255) not null,
        "not" plan not null
    );

    insert into public.clashes(email, "not")
    values
        ('aardvark@x.com', 'free'),
        ('bat@x.com', 'pro'),
        ('cat@x.com', 'enterprise'),
        ('dog@x.com', 'free'),
        ('elephant@x.com', 'pro');

    select jsonb_pretty(
        graphql.resolve($$
        {
            clashesCollection(
                filter: {
                    or: [
                        {id: {eq: 1}}
                        {not: {eq: "pro"}, and: [{id: {eq: 2}}, {email: {eq: "bat@x.com"}}]}
                    ]
                }
            ) {
                edges {
                    node {
                        id
                        email
                        not
                    }
                }
            }
        }
        $$)
    );
    rollback to savepoint a;

    -- column named `and` updates the entity's <Entity>Filter introspection schema's type
    comment on schema public is e'@graphql({"inflect_names": false})';

    create table clashes(
        id serial primary key,
        email varchar(255) not null
    );

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "clashesFilter") {
            kind
            inputFields {
              name
              type {
                kind
                name
              }
            }
          }
        }
        $$)
    );

    alter table clashes add column "and" int;

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "clashesFilter") {
            kind
            inputFields {
              name
              type {
                kind
                name
              }
            }
          }
        }
        $$)
    );

rollback;
