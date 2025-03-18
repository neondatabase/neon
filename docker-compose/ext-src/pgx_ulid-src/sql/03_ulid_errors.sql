-- Test ULID error handling

-- Test invalid ULID string (too short)
SELECT '01GV5PA9EQG7D82Q3Y4PKBZSY'::ulid;

-- Test invalid ULID string (invalid character)
SELECT '01GV5PA9EQG7D82Q3Y4PKBZSYU'::ulid;

-- Test NULL handling
SELECT 'NULL to ulid conversion returns NULL' as test_name,
       NULL::ulid IS NULL as result;

