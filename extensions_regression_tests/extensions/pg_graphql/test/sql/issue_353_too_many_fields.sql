begin;

    create table many_fields(
        id int primary key,
        field1 int,
        field2 int,
        field3 int,
        field4 int,
        field5 int,
        field6 int,
        field7 int,
        field8 int,
        field9 int,
        field10 int,
        field11 int,
        field12 int,
        field13 int,
        field14 int,
        field15 int,
        field16 int,
        field17 int,
        field18 int,
        field19 int,
        field20 int,
        field21 int,
        field22 int,
        field23 int,
        field24 int,
        field25 int,
        field26 int,
        field27 int,
        field28 int,
        field29 int,
        field30 int,
        field31 int,
        field32 int,
        field33 int,
        field34 int,
        field35 int,
        field36 int,
        field37 int,
        field38 int,
        field39 int,
        field40 int,
        field41 int,
        field42 int,
        field43 int,
        field44 int,
        field45 int,
        field46 int,
        field47 int,
        field48 int,
        field49 int,
        field50 int,
        field51 int,
        field52 int,
        field53 int,
        field54 int,
        field55 int,
        field56 int,
        field57 int,
        field58 int,
        field59 int,
        field60 int,
        field61 int,
        field62 int,
        field63 int,
        field64 int,
        field65 int,
        field66 int,
        field67 int,
        field68 int,
        field69 int,
        field70 int,
        field71 int,
        field72 int,
        field73 int,
        field74 int,
        field75 int,
        field76 int,
        field77 int,
        field78 int,
        field79 int,
        field80 int,
        field81 int,
        field82 int,
        field83 int,
        field84 int,
        field85 int,
        field86 int,
        field87 int,
        field88 int,
        field89 int,
        field90 int,
        field91 int,
        field92 int,
        field93 int,
        field94 int,
        field95 int,
        field96 int,
        field97 int,
        field98 int,
        field99 int,
        field100 int
    );

    insert into many_fields(id) values (1);

    select jsonb_pretty(
        graphql.resolve($$
            {
              manyFieldsCollection {
                edges {
                  node {
                    id
                    field1
                    field2
                    field3
                    field4
                    field5
                    field6
                    field7
                    field8
                    field9
                    field10
                    field11
                    field12
                    field13
                    field14
                    field15
                    field16
                    field17
                    field18
                    field19
                    field20
                    field21
                    field22
                    field23
                    field24
                    field25
                    field26
                    field27
                    field28
                    field29
                    field30
                    field31
                    field32
                    field33
                    field34
                    field35
                    field36
                    field37
                    field38
                    field39
                    field40
                    field41
                    field42
                    field43
                    field44
                    field45
                    field46
                    field47
                    field48
                    field49
                    field50
                    field51
                    field52
                    field53
                    field54
                    field55
                    field56
                    field57
                    field58
                    field59
                    field60
                    field61
                    field62
                    field63
                    field64
                    field65
                    field66
                    field67
                    field68
                    field69
                    field70
                    field71
                    field72
                    field73
                    field74
                    field75
                    field76
                    field77
                    field78
                    field79
                    field80
                    field81
                    field82
                    field83
                    field84
                    field85
                    field86
                    field87
                    field88
                    field89
                    field90
                    field91
                    field92
                    field93
                    field94
                    field95
                    field96
                    field97
                    field98
                    field99
                    field100
                  }
                }
              }
            }
        $$)
    );

rollback;
