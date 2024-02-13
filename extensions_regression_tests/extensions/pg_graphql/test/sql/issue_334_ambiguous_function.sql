begin;

    create table public.recipe_ingredient(
        id int primary key
    );

    create table public.recipe(
        id int primary key
    );

    insert into public.recipe(id) values (1);

    create or replace function _calories(rec public.recipe_ingredient)
        returns smallint
        stable
        language sql
    as $$
        select 1;
    $$;

    create or replace function _calories(rec public.recipe)
        returns smallint
        stable
        language sql
    as $$
        select 1;
    $$;

    select jsonb_pretty(
        graphql.resolve($$
        {
          recipeCollection {
            edges {
              node {
                id
                calories
              }
            }
          }
        }
        $$)
    );





rollback;
