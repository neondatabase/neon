-- Test conversion from timestamptz
SELECT 'timestamptz to ulid conversion' as test_name,
       '2023-03-10 04:00:49.111'::timestamptz::ulid::text = '01GV5PA9EQ0000000000000000' as result;
