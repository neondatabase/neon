begin;
    create table book(
        id int primary key,
        is_verified bool
    );

    insert into public.book(id, is_verified)
    values
        (1, true);

    savepoint a;

    -- Should be skipped
    select jsonb_pretty(
        graphql.resolve($$
            {
              bookCollection {
                edges {
                  node {
                    id
                    isVerified @skip( if: true )
                  }
                }
              }
            }
        $$)
    );

    -- Should not be skipped
    select jsonb_pretty(
        graphql.resolve($$
            {
              bookCollection {
                edges {
                  node {
                    id
                    isVerified @skip( if: false )
                  }
                }
              }
            }
        $$)
    );


    select jsonb_pretty(
        graphql.resolve($$
            query XXX($should_skip: Boolean! ){
              bookCollection{
                edges {
                  node {
                    id
                    isVerified @skip(if: $should_skip)
                  }
                }
              }
            }
          $$,
          '{"should_skip": true}'
        )
    );

    select jsonb_pretty(
        graphql.resolve($$
            query XXX($should_skip: Boolean! ){
              bookCollection{
                edges {
                  node {
                    id
                    isVerified @skip(if: $should_skip)
                  }
                }
              }
            }
          $$,
          '{"should_skip": false}'
        )
    );


    select jsonb_pretty(
        graphql.resolve($$
            query XXX($should_skip: Boolean! ){
              bookCollection{
                edges {
                  node {
                    id
                    isVerified @skip(if: $should_skip)
                  }
                }
              }
            }
          $$,
          '{}'
        )
    );

    -- Should not be skipped
    select jsonb_pretty(
        graphql.resolve($$
            {
              bookCollection {
                edges {
                  node {
                    id
                    isVerified @include( if: true )
                  }
                }
              }
            }
        $$)
    );

    -- Should be skipped
    select jsonb_pretty(
        graphql.resolve($$
            {
              bookCollection {
                edges {
                  node {
                    id
                    isVerified @include( if: false )
                  }
                }
              }
            }
        $$)
    );


    select jsonb_pretty(
        graphql.resolve($$
            query XXX($should_include: Boolean! ){
              bookCollection{
                edges {
                  node {
                    id
                    isVerified @include(if: $should_include)
                  }
                }
              }
            }
          $$,
          '{"should_include": true}'
        )
    );

    select jsonb_pretty(
        graphql.resolve($$
            query XXX($should_include: Boolean! ){
              bookCollection{
                edges {
                  node {
                    id
                    isVerified @include(if: $should_include)
                  }
                }
              }
            }
          $$,
          '{"should_include": false}'
        )
    );

    select jsonb_pretty(
        graphql.resolve($$
            query XXX($should_include: Boolean! ){
              bookCollection{
                edges {
                  node {
                    id
                    isVerified @include(if: $should_include)
                  }
                }
              }
            }
          $$,
          '{}'
        )
    );

    select jsonb_pretty(
        graphql.resolve($$
            {
              bookCollection {
                edges {
                  node {
                    id
                    verif1: isVerified @skip( if: true ) @include( if: true )
                    verif2: isVerified @skip( if: true ) @include( if: false)
                    verif3: isVerified @skip( if: false ) @include( if: true)
                    verif4: isVerified @skip( if: false ) @include( if: false)
                    verif5: isVerified @include( if: true ) @skip( if: true )
                    verif6: isVerified @include( if: true ) @skip( if: false)
                    verif7: isVerified @include( if: false ) @skip( if: true)
                    verif8: isVerified @include( if: false ) @skip( if: false)
                  }
                }
              }
            }
        $$)
    );

rollback;
