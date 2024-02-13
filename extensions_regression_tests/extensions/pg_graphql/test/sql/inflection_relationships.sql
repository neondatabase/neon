begin;
    savepoint a;
    -- Inflection true, Overrides: off, Ends with '_id'
    comment on schema public is e'@graphql({"inflect_names": true})';

    create table account_holder (
        id int primary key,
        author_id int,
        constraint fkey_author_id foreign key (author_id) references account_holder(id)
    );

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "AccountHolder") {
            fields {
                name
            }
          }
        }
        $$)
    );

    -- Inflection true, Overrides: off, does not end with '_id'
    rollback to savepoint a;

    comment on schema public is e'@graphql({"inflect_names": true})';

    create table account_holder (
        id int primary key,
        author int,
        constraint fkey_author_id foreign key (author) references account_holder(id)
    );

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "AccountHolder") {
            fields {
                name
            }
          }
        }
        $$)
    );

    -- Inflection true, Overrides: true
    rollback to savepoint a;

    comment on schema public is e'@graphql({"inflect_names": true})';

    create table account_holder (
        id int primary key,
        account_id int,
        constraint fkey_account_id foreign key (account_id) references account_holder(id)
    );

    comment on constraint fkey_account_id
    on account_holder
    is E'@graphql({"foreign_name": "auTHor", "local_name": "children"})';

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "AccountHolder") {
            fields {
                name
            }
          }
        }
        $$)
    );

    -- Inflection false, Overrides: off, Ends with 'Id'
    rollback to savepoint a;

    comment on schema public is e'@graphql({"inflect_names": false})';

    create table "AccountHolder" (
        id int primary key,
        "authorId" int,
        constraint fkey_author_id foreign key ("authorId") references "AccountHolder"(id)
    );

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "AccountHolder") {
            fields {
                name
            }
          }
        }
        $$)
    );

    -- Inflection false, Overrides: off, does not end with 'Id'
    rollback to savepoint a;

    comment on schema public is e'@graphql({"inflect_names": false})';

    create table "AccountHolder" (
        id int primary key,
        author int,
        constraint fkey_author_id foreign key (author) references "AccountHolder"(id)
    );

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "AccountHolder") {
            fields {
                name
            }
          }
        }
        $$)
    );

    -- Inflection false, Overrides: true
    rollback to savepoint a;

    comment on schema public is e'@graphql({"inflect_names": false})';

    create table "AccountHolder"(
        id int primary key,
        "accountId" int,
        constraint fkey_account_id foreign key ("accountId") references "AccountHolder"(id)
    );

    comment on constraint fkey_account_id
    on "AccountHolder"
    is E'@graphql({"foreign_name": "auTHor", "local_name": "children"})';

    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "AccountHolder") {
            fields {
                name
            }
          }
        }
        $$)
    );

rollback;
