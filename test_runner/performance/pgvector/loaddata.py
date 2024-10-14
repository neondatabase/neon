from __future__ import annotations

import sys
from pathlib import Path

import numpy as np  # type: ignore [import]
import pandas as pd  # type: ignore [import]
import psycopg2
from pgvector.psycopg2 import register_vector  # type: ignore [import]
from psycopg2.extras import execute_values


def print_usage():
    print("Usage: loaddata.py <CONNSTR> <DATADIR>")


def main(conn_str, directory_path):
    # Connection to PostgreSQL
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            # Run SQL statements
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            register_vector(conn)
            cursor.execute("DROP TABLE IF EXISTS documents;")
            cursor.execute(
                """
                CREATE TABLE documents (
                    _id TEXT PRIMARY KEY,
                    title TEXT,
                    text TEXT,
                    embeddings vector(1536) -- text-embedding-3-large-1536-embedding (OpenAI)
                );
            """
            )
            conn.commit()

            # List and sort Parquet files
            parquet_files = sorted(Path(directory_path).glob("*.parquet"))

            for file in parquet_files:
                print(f"Loading {file} into PostgreSQL")
                df = pd.read_parquet(file)

                print(df.head())

                data_list = [
                    (
                        row["_id"],
                        row["title"],
                        row["text"],
                        np.array(row["text-embedding-3-large-1536-embedding"]),
                    )
                    for index, row in df.iterrows()
                ]
                # Use execute_values to perform batch insertion
                execute_values(
                    cursor,
                    "INSERT INTO documents (_id, title, text, embeddings) VALUES %s",
                    data_list,
                )
                # Commit after we insert all embeddings
                conn.commit()

                print(f"Loaded {file} into PostgreSQL")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print_usage()
        sys.exit(1)

    conn_str = sys.argv[1]
    directory_path = sys.argv[2]
    main(conn_str, directory_path)
