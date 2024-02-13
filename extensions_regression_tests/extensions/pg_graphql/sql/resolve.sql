create or replace function graphql.resolve(
      "query" text,
      "variables" jsonb default '{}',
      "operationName" text default null,
      "extensions" jsonb default null
)
    returns jsonb
    language plpgsql
as $$
declare
    res jsonb;
    message_text text;
begin
  begin
    select graphql._internal_resolve("query" := "query",
                                     "variables" := "variables",
                                     "operationName" := "operationName",
                                     "extensions" := "extensions") into res;
    return res;
  exception
    when others then
    get stacked diagnostics message_text = message_text;
    return
    jsonb_build_object('data', null,
                       'errors', jsonb_build_array(jsonb_build_object('message', message_text)));
  end;
end;
$$;
