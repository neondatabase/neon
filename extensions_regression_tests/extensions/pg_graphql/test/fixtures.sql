drop extension if exists pg_graphql;
create extension pg_graphql cascade;
comment on schema public is '@graphql({"inflect_names": true})';


-- To remove after test suite port
create or replace function graphql.encode(jsonb)
    returns text
    language sql
    immutable
as $$
/*
    select graphql.encode('("{""(email,asc,t)"",""(id,asc,f)""}","[""aardvark@x.com"", 1]")'::graphql.cursor)
*/
    select encode(convert_to($1::text, 'utf-8'), 'base64')
$$;
