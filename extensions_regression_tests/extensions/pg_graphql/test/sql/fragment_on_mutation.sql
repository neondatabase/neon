begin;

    create table blog_post(
        id int primary key,
        title text not null
    );

    select graphql.resolve($$
        mutation {
          ...blogPosts_insert
        }

        fragment blogPosts_insert on Mutation {
          insertIntoBlogPostCollection(objects: [
            { id: 1, title: "foo" }
          ]) {
            affectedCount
            records {
              id
              title
            }
          }
        }
    $$);

    select * from blog_post;

rollback;
