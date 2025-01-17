#! /usr/bin/env python3

from __future__ import annotations

import os

import pg8000.dbapi

if __name__ == "__main__":
    kwargs = {
        k.removeprefix("NEON_").lower(): v
        for k in ("NEON_HOST", "NEON_DATABASE", "NEON_USER", "NEON_PASSWORD")
        if (v := os.environ.get(k, None)) is not None
    }
    conn = pg8000.dbapi.connect(
        **kwargs,
        ssl_context=True,
    )

    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    row = cursor.fetchone()
    print(row[0])
    conn.close()
