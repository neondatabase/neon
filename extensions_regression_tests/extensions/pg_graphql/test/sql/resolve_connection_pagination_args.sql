begin;
    create table account(
        id int primary key
    );
    comment on table account is e'@graphql({"totalCount": {"enabled": true}})';


    insert into public.account(id)
    select * from generate_series(1,5);


    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(first: 2, after: "WzNd") {
                totalCount
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

    -- First with after variable
    select jsonb_pretty(
        graphql.resolve($$
            query ABC($afterCursor: Cursor){
              accountCollection(first: 2, after: $afterCursor) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$,
        variables := '{"afterCursor": "WzNd"}'
    ));

    -- First without an after clause
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(first: 2) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

    -- First with after = null same as omitting after
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(first: 2, after: null) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

    -- First with after = null as variable same as omitting after
    select jsonb_pretty(
        graphql.resolve($$
            query ABC($afterCursor: Cursor){
              accountCollection(first: 2, after: $afterCursor) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$,
        variables := '{"afterCursor": null}'
    ));

    -- last before
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(last: 2, before: "WzNd") {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

    -- Last with after variable
    select jsonb_pretty(
        graphql.resolve($$
            query ABC($afterCursor: Cursor){
              accountCollection(last: 2, before: $afterCursor) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$,
        variables := '{"afterCursor": "WzNd"}'
    ));

    -- last without an after clause
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(last: 2) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

    -- last with before = null same as omitting after
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(last: 2, before: null) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );

    -- First with before variable
    select jsonb_pretty(
        graphql.resolve($$
            query ABC($beforeCursor: Cursor){
              accountCollection(last: 2, before: $beforeCursor) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$,
        variables := '{"beforeCursor": "WzNd"}'
    ));

    -- issue #161: confirm variables for first/last before/after may be present so long as
    -- only correct pairs are passed as variables
    select jsonb_pretty(
        graphql.resolve($$
            query ABC($beforeCursor: Cursor, $afterCursor: Cursor, $lastN: Int, $firstN: Int){
              accountCollection(last: $lastN, before: $beforeCursor, first: $firstN, after: $afterCursor) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$,
        variables := '{"beforeCursor": "WzNd", "afterCursor": null, "lastN": 2, "beforeN": null}'
    ));



    -- Last without a before clause
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(last: 2) {
                edges {
                  node {
                    id
                  }
                }
              }
            }
        $$)
    );


    -- Test interactions with orderBy
    create table blog(
        id int primary key,
        reversed int,
        title text
    );

    insert into public.blog(id, reversed, title)
    select
        x.id,
        (20 - id) % 5,
        case id % 3
            when 1 then 'a'
            when 2 then 'b'
            when 3 then null
        end
    from generate_series(1,20) x(id);

    select * from public.blog;

    -- First after w/ complex order
    select jsonb_pretty(
        graphql.resolve($$
            query ABC($afterCursor: Cursor){
              blogCollection(
                first: 5
                after: $afterCursor
                orderBy: [{reversed: AscNullsLast}, {title: AscNullsFirst}]
              ) {
                edges {
                  node {
                    id
                    reversed
                    title
                  }
                }
              }
            }
        $$,
        jsonb_build_object('afterCursor', graphql.encode('[3, "a"]'::jsonb))
        )
    );

    -- Last before w/ complex order
    select jsonb_pretty(
        graphql.resolve($$
            query ABC($beforeCursor: Cursor){
              blogCollection(
                last: 5
                before: $beforeCursor
                orderBy: [{reversed: AscNullsLast}, {title: AscNullsFirst}]
              ) {
                edges {
                  node {
                    id
                    reversed
                    title
                  }
                }
              }
            }
        $$,
        jsonb_build_object('beforeCursor', graphql.encode('[3, "a"]'::jsonb))
        )
    );

    /*
    ERROR STATES
    */

    -- first + last raises an error
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(first: 2, last: 1) {
                totalCount
              }
            }
        $$)
    );

    -- before + after raises an error
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(before: "WzNd", after: "WzNd") {
                totalCount
              }
            }
        $$)
    );

    -- first + before raises an error
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(first: 2, before: "WzNd") {
                totalCount
              }
            }
        $$)
    );

    -- last + after raises an error
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(last: 2, after: "WzNd") {
                totalCount
              }
            }
        $$)
    );


rollback;
