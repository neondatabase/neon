begin;
    -- https://github.com/supabase/pg_graphql/issues/225

    create table post(
        id int primary key,
        title text
    );

    insert into public.post(id, title)
    select x.id, (10-x.id)::text from generate_series(1,3) x(id);

    select jsonb_pretty(
        graphql.resolve($$
            {
              postCollection( orderBy: [{id: DescNullsFirst, title: null}]) {
                edges {
                  node {
                    id
                    title
                  }
                }
              }
            }
        $$)
    );

    select jsonb_pretty(
        graphql.resolve($$
            {
              postCollection( orderBy: [{id: null, title: DescNullsLast}]) {
                edges {
                  node {
                    id
                    title
                  }
                }
              }
            }
        $$)
    );

    select jsonb_pretty(
        graphql.resolve($$
            {
              postCollection( orderBy: [{id: null}, { title: DescNullsLast}]) {
                edges {
                  node {
                    id
                    title
                  }
                }
              }
            }
        $$)
    );


rollback;
