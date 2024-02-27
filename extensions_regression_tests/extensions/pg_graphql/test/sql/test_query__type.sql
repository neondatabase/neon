select graphql.resolve(
    query:='query Abc { __type(name: "Int") { name kind description } }'
);
