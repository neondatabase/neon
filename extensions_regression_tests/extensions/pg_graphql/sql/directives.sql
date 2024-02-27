create function graphql.comment_directive(comment_ text)
    returns jsonb
    language sql
    immutable
as $$
    /*
    comment on column public.account.name is '@graphql.name: myField'
    */
    select
        coalesce(
            (
                regexp_match(
                    comment_,
                    '@graphql\((.+?)\)'
                )
            )[1]::jsonb,
            jsonb_build_object()
        )
$$;
