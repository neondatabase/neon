from fixtures.neon_fixtures import NeonEnv

SIGNED_CHAR_EXTRACT = """
    WITH
  pagenumbers AS (
    SELECT num FROM generate_series(0, (pg_relation_size('test_payload_idx') / 8192) - 1) it(num)
  )
    SELECT num,
    substr(page, 9, 8192-8),
    (gin_page_opaque_info(page)).*
    FROM pagenumbers,
    LATERAL (SELECT get_raw_page('test_payload_idx', num)) AS p(page)
    WHERE ARRAY['leaf'] = ((gin_page_opaque_info(page)).flags);
    """

def test_signed_char(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    ses1 = endpoint.connect().cursor()
    # Add the required extensions
    ses1.execute("CREATE EXTENSION pg_trgm;")
    ses1.execute("CREATE EXTENSION pageinspect;")
    # Create a test table
    ses1.execute("CREATE TABLE test (payload text);")
    # Create a GIN based index
    ses1.execute(
        "CREATE INDEX test_payload_idx ON test USING gin (payload gin_trgm_ops) WITH (gin_pending_list_limit = 64);"
    )
    # insert a multibyte character to trigger order-dependent hashing
    ses1.execute(
        "INSERT INTO test SELECT '123456789BV' || CHR(127153) /* ace of spades, a multibyte character */ || i::text from generate_series(1, 40) as i(i);"
    )
    ses1.execute(SIGNED_CHAR_EXTRACT)
    pages = ses1.fetchall()
    # Expected output is [(1, <memory at 0x1098c7a00>, 4294967295, 0, ['leaf'])]
    page1 = pages[0]
    assert page1[0] == 1
    assert page1[-1] == ['leaf']
    assert page1[-2] == 0
    assert page1[-3] == 4294967295