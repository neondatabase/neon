create or replace function graphql.exception(message text)
    returns text
    language plpgsql
as $$
begin
    raise exception using errcode='22000', message=message;
end;
$$;
