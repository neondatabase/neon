begin;
    comment on schema public is '@graphql({"inflect_names": false})';

    create type "Color" as enum ('RED', 'GREEN');

    create table "MainType" (
        id serial primary key,
        "typeJson" json,
        "typeJsonb" jsonb,
        "typeBigint" bigint,
        "typeText" text,
        "typeVarchar" varchar,
        "typeVarchar_n" varchar(10),
        "typeChar" char,
        "typeUuid" uuid,
        "typeDate" date,
        "typeTime" time,
        "typeDatetime" timestamptz,
        "typeEnum" "Color",
        "typeNumeric" numeric(10,2),
        "typeFloat" float,
        "typeDouble" double precision,
        "typeArrayEnum" "Color"[],
        "typeArrayText" text[],
        "typeArrayJson" json[],
        "typeArrayNumeric" numeric(10,2)[]
    );

    -- insert works with large range of types
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              insertIntoMainTypeCollection (
                objects: [
                  {
                    typeJson: "{\"a\": 1}"
                    typeJsonb: "{\"a\": 1}"
                    typeBigint: "1"
                    typeText: "1a"
                    typeVarchar: "1a"
                    typeVarchar_n: "1a"
                    typeChar: "1"
                    typeUuid: "d4f47ef5-fb25-4d2a-83b2-d5fab114a2e6"
                    typeDate: "1900-01-01"
                    typeTime: "18:23"
                    typeDatetime: "1900-01-01 18:00"
                    typeEnum: RED
                    typeFloat: 25.1
                    typeDouble: 25
                    typeNumeric: "25.1"
                    typeArrayEnum: [RED, RED, GREEN]
                    typeArrayText: ["a", "b"]
                    typeArrayJson: ["{\"a\": 1}"]
                    typeArrayNumeric: ["2.5"]

                  }
              ]
              ) {
                affectedCount
                records{
                  id
                  typeJson
                  typeJsonb
                  typeBigint
                  typeText
                  typeVarchar
                  typeVarchar_n
                  typeChar
                  typeUuid
                  typeDate
                  typeTime
                  typeDatetime
                  typeEnum
                  typeFloat
                  typeDouble
                  typeNumeric
                }
              }
            }
        $$)
    );

    select * from "MainType";

    -- update works with large range of types
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              updateMainTypeCollection (
                filter: {id: {eq: 1}}
                set: {
                    typeJson: "{\"b\": 2}"
                    typeJsonb: "{\"b\": 2}"
                    typeBigint: "1"
                    typeText: "b2"
                    typeVarchar: "b2"
                    typeVarchar_n: "b2"
                    typeChar: "b"
                    typeUuid: "e4f47ef5-fb25-4d2a-83b2-d5fab114a2e6"
                    typeDate: "2000-01-01"
                    typeTime: "17:23"
                    typeDatetime: "2000-01-01 18:00"
                    typeEnum: GREEN
                    typeFloat: 30
                    typeDouble: 21.5
                    typeNumeric: "0.1"
                    typeArrayEnum: [GREEN],
                    typeArrayText: ["c", "d"]
                    typeArrayJson: ["{\"b\": 2}"]
                    typeArrayNumeric: ["0.1"]
                }
              ) {
                affectedCount
                records{
                  id
                  typeJson
                  typeJsonb
                  typeBigint
                  typeText
                  typeVarchar
                  typeVarchar_n
                  typeChar
                  typeUuid
                  typeDate
                  typeTime
                  typeDatetime
                  typeEnum
                  typeFloat
                  typeDouble
                  typeNumeric
                }
              }
            }
        $$)
    );

    select * from "MainType";

    -- Select/OrderBy works with all types
    select jsonb_pretty(
        graphql.resolve($$
            {
              mainTypeCollection (
                orderBy: [
                  {id: AscNullsLast}
                  {typeBigint: AscNullsLast}
                  {typeText: AscNullsLast}
                  {typeVarchar: AscNullsLast}
                  {typeVarchar_n: AscNullsLast}
                  {typeUuid: AscNullsLast}
                  {typeDate: AscNullsLast}
                  {typeTime: AscNullsLast}
                  {typeDatetime: AscNullsLast}
                  {typeEnum: AscNullsLast}
                  {typeFloat: AscNullsLast}
                  {typeDouble: AscNullsLast}
                  {typeNumeric: AscNullsLast}
                ]
              ) {
                  pageInfo{
                    startCursor
                    endCursor
                  }
                  edges{
                    node{
                      id
                    }
                  }
                }
            }
        $$)
    );


    -- Delete/Filter works with all types
    select jsonb_pretty(
        graphql.resolve($$
            mutation {
              deleteFromMainTypeCollection (
                filter: {
                    typeBigint: {eq: "1"}
                    typeText: {eq: "b2"}
                    typeVarchar: {eq: "b2"}
                    typeVarchar_n: {eq: "b2"}
                    typeChar: {eq: "b"}
                    typeUuid: {eq: "e4f47ef5-fb25-4d2a-83b2-d5fab114a2e6"}
                    typeDate: {eq: "2000-01-01"}
                    typeTime: {eq: "17:23"}
                    typeDatetime: {eq: "2000-01-01 18:00"}
                    typeEnum: {eq: GREEN}
                    typeNumeric: {eq: 30}
                    typeNumeric: {eq: 21.5}
                    typeNumeric: {eq: "0.1"}
                }
              ) {
                affectedCount
                records{
                  id
                  typeJson
                  typeJsonb
                  typeBigint
                  typeText
                  typeVarchar
                  typeVarchar_n
                  typeChar
                  typeUuid
                  typeEnum
                  typeFloat
                  typeDouble
                  typeNumeric
                }
              }
            }
        $$)
    );

    -- Check field types
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "MainType") {
            kind
            fields {
                name
                type {
                    kind
                    name
                    ofType {
                        kind
                        name
                    }
                }
            }
          }
        }
        $$)
    );


    -- Check field types
    select jsonb_pretty(
        graphql.resolve($$
        {
          __type(name: "MainTypeFilter") {
            kind
            inputFields {
                name
                type {
                    kind
                    name
                    ofType {
                        kind
                        name
                    }
                }
            }
          }
        }
        $$)
    );

rollback;
