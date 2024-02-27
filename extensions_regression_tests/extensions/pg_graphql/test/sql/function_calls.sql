begin;

    savepoint a;
    -- Only volatilve functions appear on the mutation object

    create function add_smallints(a smallint, b smallint)
        returns smallint language sql volatile
    as $$ select a + b; $$;

    comment on function add_smallints is e'@graphql({"description": "adds two smallints"})';

    select jsonb_pretty(graphql.resolve($$
        mutation {
            addSmallints(a: 1, b: 2)
        }
    $$));

    create function add_ints(a int, b int)
        returns int language sql volatile
    as $$ select a + b; $$;

    comment on function add_ints is e'@graphql({"name": "intsAdd"})';

    select jsonb_pretty(graphql.resolve($$
        mutation {
            intsAdd(a: 2, b: 3)
        }
    $$));

    comment on schema public is e'@graphql({"inflect_names": false})';

    create function add_bigints(a bigint, b bigint)
        returns bigint language sql volatile
    as $$ select a + b; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            add_bigints(a: 3, b: 4)
        }
    $$));

    comment on schema public is e'@graphql({"inflect_names": true})';

    create function add_reals(a real, b real)
        returns real language sql volatile
    as $$ select a + b; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            addReals(a: 4.5, b: 5.6)
        }
    $$));

    create function add_doubles(a double precision, b double precision)
        returns double precision language sql volatile
    as $$ select a + b; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            addDoubles(a: 7.8, b: 9.1)
        }
    $$));

    create function add_numerics(a numeric, b numeric)
        returns numeric language sql volatile
    as $$ select a + b; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            addNumerics(a: "11.12", b: "13.14")
        }
    $$));

    create function and_bools(a bool, b bool)
        returns bool language sql volatile
    as $$ select a and b; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            andBools(a: true, b: false)
        }
    $$));

    create function uuid_identity(input uuid)
        returns uuid language sql volatile
    as $$ select input; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            uuidIdentity(input: "d3ef3a8c-2c72-11ee-b094-776acede7221")
        }
    $$));

    create function concat_text(a text, b text)
        returns text language sql volatile
    as $$ select a || b; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            concatText(a: "Hello ", b: "World")
        }
    $$));

    create function next_day(d date)
        returns date language sql volatile
    as $$ select d + interval '1 day'; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            nextDay(d: "2023-07-28")
        }
    $$));

    create function next_hour(t time)
        returns time language sql volatile
    as $$ select t + interval '1 hour'; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            nextHour(t: "10:20")
        }
    $$));

    set time zone 'Asia/Kolkata'; -- same as IST

    create function next_hour_with_timezone(t time with time zone)
        returns time with time zone language sql volatile
    as $$ select t + interval '1 hour'; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            nextHourWithTimezone(t: "10:20+05:30")
        }
    $$));

    create function next_minute(t timestamp)
        returns timestamp language sql volatile
    as $$ select t + interval '1 minute'; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            nextMinute(t: "2023-07-28 12:39:05")
        }
    $$));

    create function next_minute_with_timezone(t timestamptz)
        returns timestamptz language sql volatile
    as $$ select t + interval '1 minute'; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            nextMinuteWithTimezone(t: "2023-07-28 12:39:05+05:30")
        }
    $$));

    create function get_json_obj(input json, key text)
        returns json language sql volatile
    as $$ select input -> key; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            getJsonObj(input: "{\"a\": {\"b\": \"foo\"}}", key: "a")
        }
    $$));

    create function get_jsonb_obj(input jsonb, key text)
        returns jsonb language sql volatile
    as $$ select input -> key; $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            getJsonbObj(input: "{\"a\": {\"b\": \"foo\"}}", key: "a")
        }
    $$));

    create function concat_chars(a char(2), b char(3))
        returns char(5) language sql volatile
    as $$ select (a::char(2) || b::char(3))::char(5); $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            concatChars(a: "He", b: "llo")
        }
    $$));

    create function concat_varchars(a varchar(2), b varchar(3))
        returns varchar(5) language sql volatile
    as $$ select (a::varchar(2) || b::varchar(3))::varchar(5); $$;

    select jsonb_pretty(graphql.resolve($$
        mutation {
            concatVarchars(a: "He", b: "llo")
        }
    $$));

    select jsonb_pretty(graphql.resolve($$
    query IntrospectionQuery {
        __schema {
            mutationType {
                fields {
                    name
                    description
                    type {
                        kind
                    }
                    args {
                        name
                        type {
                            name
                        }
                    }
                }
            }
        }
    } $$));

    rollback to savepoint a;

    -- Only stable and immutable functions appear on the query object

    create function add_smallints(a smallint, b smallint)
        returns smallint language sql stable
    as $$ select a + b; $$;

    comment on function add_smallints is e'@graphql({"description": "returns a + b"})';

    select jsonb_pretty(graphql.resolve($$
        query {
            addSmallints(a: 1, b: 2)
        }
    $$));

    create function add_ints(a int, b int)
        returns int language sql immutable
    as $$ select a + b; $$;

    comment on function add_ints is e'@graphql({"name": "intsAdd"})';

    select jsonb_pretty(graphql.resolve($$
        query {
            intsAdd(a: 2, b: 3)
        }
    $$));

    create function add_bigints(a bigint, b bigint)
        returns bigint language sql stable
    as $$ select a + b; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            addBigints(a: 3, b: 4)
        }
    $$));

    create function add_reals(a real, b real)
        returns real language sql immutable
    as $$ select a + b; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            addReals(a: 4.5, b: 5.6)
        }
    $$));

    create function add_doubles(a double precision, b double precision)
        returns double precision language sql stable
    as $$ select a + b; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            addDoubles(a: 7.8, b: 9.1)
        }
    $$));

    create function add_numerics(a numeric, b numeric)
        returns numeric language sql immutable
    as $$ select a + b; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            addNumerics(a: "11.12", b: "13.14")
        }
    $$));

    create function and_bools(a bool, b bool)
        returns bool language sql stable
    as $$ select a and b; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            andBools(a: true, b: false)
        }
    $$));

    create function uuid_identity(input uuid)
        returns uuid language sql immutable
    as $$ select input; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            uuidIdentity(input: "d3ef3a8c-2c72-11ee-b094-776acede7221")
        }
    $$));

    create function concat_text(a text, b text)
        returns text language sql stable
    as $$ select a || b; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            concatText(a: "Hello ", b: "World")
        }
    $$));

    create function next_day(d date)
        returns date language sql immutable
    as $$ select d + interval '1 day'; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            nextDay(d: "2023-07-28")
        }
    $$));

    create function next_hour(t time)
        returns time language sql stable
    as $$ select t + interval '1 hour'; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            nextHour(t: "10:20")
        }
    $$));

    set time zone 'Asia/Kolkata'; -- same as IST

    create function next_hour_with_timezone(t time with time zone)
        returns time with time zone language sql immutable
    as $$ select t + interval '1 hour'; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            nextHourWithTimezone(t: "10:20+05:30")
        }
    $$));

    create function next_minute(t timestamp)
        returns timestamp language sql stable
    as $$ select t + interval '1 minute'; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            nextMinute(t: "2023-07-28 12:39:05")
        }
    $$));

    create function next_minute_with_timezone(t timestamptz)
        returns timestamptz language sql immutable
    as $$ select t + interval '1 minute'; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            nextMinuteWithTimezone(t: "2023-07-28 12:39:05+05:30")
        }
    $$));

    create function get_json_obj(input json, key text)
        returns json language sql stable
    as $$ select input -> key; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            getJsonObj(input: "{\"a\": {\"b\": \"foo\"}}", key: "a")
        }
    $$));

    create function get_jsonb_obj(input jsonb, key text)
        returns jsonb language sql immutable
    as $$ select input -> key; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            getJsonbObj(input: "{\"a\": {\"b\": \"foo\"}}", key: "a")
        }
    $$));

    create function concat_chars(a char(2), b char(3))
        returns char(5) language sql stable
    as $$ select (a::char(2) || b::char(3))::char(5); $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            concatChars(a: "He", b: "llo")
        }
    $$));

    create function concat_varchars(a varchar(2), b varchar(3))
        returns varchar(5) language sql immutable
    as $$ select (a::varchar(2) || b::varchar(3))::varchar(5); $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            concatVarchars(a: "He", b: "llo")
        }
    $$));

    select jsonb_pretty(graphql.resolve($$
    query IntrospectionQuery {
        __schema {
            queryType {
                fields {
                    name
                    description
                    type {
                        kind
                    }
                    args {
                        name
                        type {
                            name
                        }
                    }
                }
            }
        }
    } $$));


    rollback to savepoint a;

    create table account(
        id serial primary key,
        email varchar(255) not null
    );

    create function returns_account()
        returns account language sql stable
    as $$ select id, email from account; $$;

    insert into account(email)
    values
        ('aardvark@x.com'),
        ('bat@x.com'),
        ('cat@x.com');

    comment on table account is e'@graphql({"totalCount": {"enabled": true}})';

    select jsonb_pretty(graphql.resolve($$
        query {
            returnsAccount {
                id
                email
                nodeId
                __typename
            }
        }
    $$));

    select jsonb_pretty(graphql.resolve($$
        query {
            returnsAccount {
                email
                nodeId
            }
        }
    $$));

    comment on schema public is e'@graphql({"inflect_names": false})';

    create function returns_account_with_id(id_to_search int)
        returns account language sql stable
    as $$ select id, email from account where id = id_to_search; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            returns_account_with_id(id_to_search: 1) {
                email
            }
        }
    $$));

    select jsonb_pretty(graphql.resolve($$
        query {
            returns_account_with_id(id_to_search: 42) { # search a non-existent id
                id
                email
                nodeId
            }
        }
    $$));

    comment on schema public is e'@graphql({"inflect_names": true})';

    select jsonb_pretty(graphql.resolve($$
        query {
            returnsAccountWithId(idToSearch: 1) {
                email
            }
        }
    $$));

    create function returns_setof_account(top int)
        returns setof account language sql stable
    as $$ select id, email from account limit top; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            returnsSetofAccount(top: 2, last: 1) {
                pageInfo {
                    startCursor
                    endCursor
                    hasNextPage
                    hasPreviousPage
                }
                edges {
                    cursor
                    node {
                        nodeId
                        id
                        email
                        __typename
                    }
                    __typename
                }
                totalCount
                __typename
            }
        }
    $$));

    -- functions with args named `first`, `last`, `before`, `after`, `filter`, or `orderBy` are not exposed
    create function arg_named_first(first int)
        returns setof account language sql stable
    as $$ select id, email from account; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            argNamedFirst {
                __typename
            }
        }
    $$));

    create function arg_named_last(last int)
        returns setof account language sql stable
    as $$ select id, email from account; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            argNamedLast {
                __typename
            }
        }
    $$));

    create function arg_named_before(before int)
        returns setof account language sql stable
    as $$ select id, email from account; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            argNamedBefore {
                __typename
            }
        }
    $$));

    create function arg_named_after(after int)
        returns setof account language sql stable
    as $$ select id, email from account; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            argNamedAfter {
                __typename
            }
        }
    $$));

    create function arg_named_filter(filter int)
        returns setof account language sql stable
    as $$ select id, email from account; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            argNamedFilter {
                __typename
            }
        }
    $$));

    create function "arg_named_orderBy"("orderBy" int)
        returns setof account language sql stable
    as $$ select id, email from account; $$;

    select jsonb_pretty(graphql.resolve($$
        query {
            argNamedOrderBy {
                __typename
            }
        }
    $$));

    select jsonb_pretty(graphql.resolve($$
    query IntrospectionQuery {
        __schema {
            queryType {
                fields {
                    name
                    description
                    type {
                        kind
                    }
                    args {
                        name
                        type {
                            name
                        }
                    }
                }
            }
        }
    } $$));

    rollback to savepoint a;

    -- Confirm that internal functions on the supabase default search_path
    -- are excluded from GraphQL API
    create schema if not exists auth;
    create schema if not exists extensions;
    create schema if not exists graphql_public;
    -- functions in the counter_example schema should be visible
    create schema if not exists counter_example;

    -- Create a function in each excluded schema to confirm it isn't visible
    create function graphql.should_be_invisible_one()
        returns smallint language sql immutable
    as $$ select 10; $$;

    create function graphql_public.should_be_invisible_two()
        returns smallint language sql immutable
    as $$ select 10; $$;

    create function auth.should_be_invisible_three()
        returns smallint language sql immutable
    as $$ select 10; $$;

    create function extensions.should_be_invisible_four()
        returns smallint language sql immutable
    as $$ select 10; $$;

    -- Create a function in counter_example which should be visible to
    -- preclude the posibility of some other error preventing the above schemas
    -- from being visible
    create function counter_example.visible()
        returns smallint language sql immutable
    as $$ select 10; $$;

    -- Set search path to include all supabase system schemas + the counter example
    set search_path = public, graphql, graphql_public, auth, extensions, counter_example;

    -- Show all fields available on the QueryType. Only the function "visible" should be seen
    -- in the output
    select jsonb_pretty(
        graphql.resolve($$
            query IntrospectionQuery {
              __schema {
                queryType {
                  name
                  fields {
                    name
                  }
                }
                mutationType {
                  name
                  fields {
                    name
                  }
                }
              }
            }
        $$)
    );

    set search_path to default;

rollback;
