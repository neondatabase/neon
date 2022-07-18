#! /usr/bin/env python3

import asyncio
import os

import asyncpg


async def run(**kwargs) -> asyncpg.Record:
    conn = await asyncpg.connect(
        **kwargs,
        statement_cache_size=0,  # Prepared statements doesn't work pgbouncer
    )
    rv = await conn.fetchrow("SELECT 1")
    await conn.close()

    return rv


if __name__ == "__main__":
    kwargs = {
        k.lstrip("NEON_").lower(): v
        for k in ("NEON_HOST", "NEON_DATABASE", "NEON_USER", "NEON_PASSWORD")
        if (v := os.environ.get(k, None)) is not None
    }

    loop = asyncio.new_event_loop()
    row = loop.run_until_complete(run(**kwargs))

    print(row[0])
