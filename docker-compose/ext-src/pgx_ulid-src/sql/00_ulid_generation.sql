-- Test basic ULID generation

-- Test gen_ulid() function
SELECT 'gen_ulid() returns a non-null value' as test_name,
       gen_ulid() IS NOT NULL as result;

-- Test that multiple calls to gen_ulid() return different values
SELECT 'gen_ulid() returns unique values' as test_name,
       gen_ulid() != gen_ulid() as result;

-- Test that gen_ulid() returns a value with the correct format
SELECT 'gen_ulid() returns correctly formatted value' as test_name,
       length(gen_ulid()::text) = 26 as result;

-- Test monotonic ULID generation
SELECT 'gen_monotonic_ulid() returns a non-null value' as test_name,
       gen_monotonic_ulid() IS NOT NULL as result;

-- Test that multiple calls to gen_monotonic_ulid() return different values
SELECT 'gen_monotonic_ulid() returns unique values' as test_name,
       gen_monotonic_ulid() != gen_monotonic_ulid() as result;

-- Test that gen_monotonic_ulid() returns a value with the correct format
SELECT 'gen_monotonic_ulid() returns correctly formatted value' as test_name,
       length(gen_monotonic_ulid()::text) = 26 as result;

-- Test that monotonic ULIDs are ordered correctly
SELECT 'gen_monotonic_ulid() returns ordered values' as test_name,
       u1 < u2 as result
FROM (
    SELECT gen_monotonic_ulid() as u1, gen_monotonic_ulid() as u2
) subq;
