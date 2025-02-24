from pathlib import Path

from fixtures.neon_fixtures import NeonEnv

SIGNED_CHAR_EXTRACT = """
    WITH
    -- Generates an intermediate table with block numbers of the index
  pagenumbers AS (
    SELECT num FROM generate_series(0, (pg_relation_size('test_payload_idx') / 8192) - 1) it(num)
  )
    SELECT num,
    -- Gets the data of the page, skipping the first 8 bytes which is the LSN
    substr(page, 9, 8192-8),
    -- Returns information about the GIN index opaque area
    (gin_page_opaque_info(page)).*
    FROM pagenumbers,
    -- Gets a page from the respective blocks of the table
    LATERAL (SELECT get_raw_page('test_payload_idx', num)) AS p(page)
    -- Filters to only return leaf pages from the GIN Index
    WHERE ARRAY['leaf'] = ((gin_page_opaque_info(page)).flags);
    """


def test_signed_char(neon_simple_env: NeonEnv):
    """
    Test that postgres was compiled with -fsigned-char.
    ---
    In multi-character keys, the GIN index creates a CRC Hash of the first 3 bytes of the key.
    The hash can have the first bit to be set or unset, needing to have a consistent representation
    of char across architectures for consistent results. GIN stores these keys by their hashes
    which determines the order in which the keys are obtained from the GIN index.
    Using -fsigned-char enforces this order across platforms making this consistent.
    The following query gets all the data present in the leaf page of a GIN index,
    which is ordered by the CRC hash and is consistent across platforms.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    with endpoint.connect().cursor() as ses1:
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
    # Compare expected output
    page1 = pages[0]
    data = bytes(page1[1]).hex()
    with open(Path(__file__).parent / "data" / "test_signed_char.out", encoding="utf-8") as f:
        expected = f.read().rstrip()

    assert data == expected
