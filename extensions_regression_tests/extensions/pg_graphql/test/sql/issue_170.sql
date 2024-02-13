begin;
    create table account(
        id int primary key
    );

    insert into public.account(id)
    select * from generate_series(1,5);

    -- hasPreviousPage is true when `after` is first element of collection
    -- "WzFd" is id=1
    -- because result set does not include the record id = 1

    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(first: 2, after: "WzFd") {
                pageInfo{
                  hasPreviousPage
                }
              }
            }
        $$)
    );

    -- hasPreviousPage is false when `after` is before the first element of collection
    -- "WzFd" is id=0
    select jsonb_pretty(
        graphql.resolve($$
            {
              accountCollection(first: 2, after: "WzBd") {
                pageInfo{
                  hasPreviousPage
                }
              }
            }
        $$)
    );
rollback;
