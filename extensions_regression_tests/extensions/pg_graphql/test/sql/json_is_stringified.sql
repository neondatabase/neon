begin;

    create table blog_post(
        id int primary key,
        data jsonb,
        parent_id int references blog_post(id)
    );

    select graphql.resolve($$
    mutation {
      insertIntoBlogPostCollection(objects: [{
        id: 1
        data: "{\"key\": \"value\"}"
        parentId: 1
      }]) {
        records {
          id
          data
        }
      }
    }
    $$);

    select * from blog_post;

    select graphql.resolve($$
    mutation {
      updateBlogPostCollection(set: {
        data: "{\"key\": \"value2\"}"
      }) {
        records {
          id
          data
        }
      }
    }
    $$);

    select * from blog_post;

    select graphql.resolve($$
    {
      blogPostCollection {
        edges {
          node {
            data
            parent {
              id
              data
            }
          }
        }
      }
    }
    $$);

    select graphql.resolve($$
    mutation {
      deleteFromBlogPostCollection {
        records {
          id
          data
        }
      }
    }
    $$);

rollback;
