begin;
    -- Disable messages that display the prepared statement SQL
    set client_min_messages to error;
    set log_min_messages to panic;

    create table account(
        id int primary key,
        email text,
        is_verified bool
    );

    savepoint a;

    -- Unknown arg is ignored
    select graphql.resolve($a$
        {
          accountCollection(
            unknown: " ; $$ drop table public.account"
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal first
    select graphql.resolve($a$
        {
          accountCollection(
            first: " ; ' $$ "
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal last
    select graphql.resolve($a$
        {
          accountCollection(
            last: " ; ' $$ "
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal cursor: nonsense input
    select graphql.resolve($a$
        {
          accountCollection(
            before: " ; ' $$ "
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal cursor: decodes, but invalid
    with curs(x) as (
        select graphql.encode('[" $$ '' ", " $$ '' "]'::jsonb)
    )
    select
        graphql.resolve(
            format($a$
                {
                  accountCollection(
                    before: "%s"
                  ) { edges { node { id } } }
                }$a$,
                curs.x
            )
        )
    from
        curs;
    rollback to savepoint a;

    -- Literal filter 1
    select graphql.resolve($a$
        {
          accountCollection(
            filter: " $$;' "
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal filter 2
    select graphql.resolve($a$
        {
          accountCollection(
            filter: {id: " $$;' "}
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal filter 3
    select graphql.resolve($a$
        {
          accountCollection(
            filter: {email: {eq: " $$;' "}}
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal orderBy 1
    select graphql.resolve($a$
        {
          accountCollection(
            orderBy: " $$;' "
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal orderBy 2
    select graphql.resolve($a$
        {
          accountCollection(
            orderBy: [" $$;' "]
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Literal orderBy 3
    select graphql.resolve($a$
        {
          accountCollection(
            orderBy: [{email: " $$;' "}]
          ) { edges { node { id } } }
        }
    $a$);
    rollback to savepoint a;

    -- Variable first
    select graphql.resolve($a$
        query Abc($var: Int!) {
          accountCollection(
            first: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": " $$;'' "}'::jsonb);
    rollback to savepoint a;

    -- Variable last
    select graphql.resolve($a$
        query Abc($var: Int!) {
          accountCollection(
            last: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": " $$;'' "}'::jsonb);
    rollback to savepoint a;

    -- Variable filter 1
    select graphql.resolve($a$
        query Abc($var: Int!) {
          accountCollection(
            filter: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": " $$;'' "}'::jsonb);
    rollback to savepoint a;

    -- Variable filter 2
    select graphql.resolve($a$
        query Abc($var: Int!) {
          accountCollection(
            filter: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": [" $$;'' "]}'::jsonb);
    rollback to savepoint a;

    -- Variable filter 3
    select graphql.resolve($a$
        query Abc($var: Int!) {
          accountCollection(
            filter: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": {"email": " $$;'' "}}'::jsonb);
    rollback to savepoint a;

    -- Variable filter 4
    select graphql.resolve($a$
        query Abc($var: Int!) {
          accountCollection(
            filter: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": {"email": {"eq": " $$;'' "}}}'::jsonb);
    rollback to savepoint a;

    -- Variable orderBy 1
    select graphql.resolve($a$
        query Abc($var: AccountFilter) {
          accountCollection(
            orderBy: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": " $$;'' "}'::jsonb);
    rollback to savepoint a;

    -- Variable orderBy 2
    select graphql.resolve($a$
        query Abc($var: AccountFilter) {
          accountCollection(
            orderBy: {id: $var}
          ) { edges { node { id } } }
        }
    $a$, '{"var": " $$;'' "}'::jsonb);
    rollback to savepoint a;

    -- Variable orderBy 3
    select graphql.resolve($a$
        query Abc($var: AccountFilter) {
          accountCollection(
            orderBy: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": [" $$;'' "]}'::jsonb);
    rollback to savepoint a;

    -- Variable orderBy 4
    select graphql.resolve($a$
        query Abc($var: AccountFilter) {
          accountCollection(
            orderBy: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": {"email": " $$;'' "}}'::jsonb);
    rollback to savepoint a;

    -- Variable orderBy 5
    select graphql.resolve($a$
        query Abc($var: AccountFilter) {
          accountCollection(
            orderBy: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": [{" ;'' ": "AscNullsFirst"}]}'::jsonb);
    rollback to savepoint a;

    -- Variable orderBy 5
    select graphql.resolve($a$
        query Abc($var: AccountFilter) {
          accountCollection(
            orderBy: $var
          ) { edges { node { id } } }
        }
    $a$, '{"var": [{"email": " $$;'' "}]}'::jsonb);
    rollback to savepoint a;
rollback;
