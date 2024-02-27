begin;

    create table account(
        id serial primary key,
        encrypted_password varchar(255) not null,
        parent_id int references account(id)
    );

    create role api;

    -- Grant access to GQL
    grant usage on schema graphql to api;
    grant all on all tables in schema graphql to api;

    -- Allow access to public.account.id but nothing else
    grant usage on schema public to api;
    grant all on all tables in schema public to api;

    savepoint a;

    -- Nothing is excluded
    set role api;
    select jsonb_pretty(graphql.resolve(' {__type(name: "Query") { fields { name } } } ') );
    select jsonb_pretty(graphql.resolve(' {__type(name: "Mutation") { fields { name } } } ') );
    rollback to savepoint a;

    -- Revoke Select Excludes: All entity types
    revoke select on public.account from api;
    set role api;
    select jsonb_pretty(graphql.resolve(' {__type(name: "Query") { fields { name } } } ') );
    select jsonb_pretty(graphql.resolve(' {__type(name: "Mutation") { fields { name } } } ') );
    rollback to savepoint a;

    -- Revoke Insert Excludes: CreateNode
    revoke insert on public.account from api;
    set role api;
    select jsonb_pretty(graphql.resolve(' {__type(name: "Query") { fields { name } } } ') );
    select jsonb_pretty(graphql.resolve(' {__type(name: "Mutation") { fields { name } } } ') );
    rollback to savepoint a;

    -- Revoke Update Excludes: UpdateNode
    revoke update on public.account from api;
    set role api;
    select jsonb_pretty(graphql.resolve(' {__type(name: "Query") { fields { name } } } ') );
    select jsonb_pretty(graphql.resolve(' {__type(name: "Mutation") { fields { name } } } ') );
    rollback to savepoint a;

    -- Revoke Delete Excludes: from Mutation schema
    revoke delete on public.account from api;
    set role api;
    select jsonb_pretty(graphql.resolve(' {__type(name: "Query") { fields { name } } } ') );
    select jsonb_pretty(graphql.resolve(' {__type(name: "Mutation") { fields { name } } } ') );
    rollback to savepoint a;

rollback;
