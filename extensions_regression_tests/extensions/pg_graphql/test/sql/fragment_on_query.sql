begin;

    create table blog_post(
        id int primary key,
        title text not null
    );

    select graphql.resolve($$
        query {
          ...blogPosts_query
        }

        fragment blogPosts_query on Query {
          blogPostCollection(first:2) {
            edges
            {
              node {
                id
                title
              }
            }
          }
        }
    $$);

rollback;
