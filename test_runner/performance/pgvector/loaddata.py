import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pgvector
from pgvector.psycopg2 import register_vector
import os
import numpy as np

directory_path = '/Users/neon/work/vanilla/dbpedia-entities-openai3-text-embedding-3-large-1536-1M/data'


# Connection to PostgreSQL

with psycopg2.connect('postgresql://neondb_owner:<secret>@ep-floral-thunder-w1gzhaxi.eu-west-1.aws.neon.build/neondb?sslmode=require') as conn:
    register_vector(conn)
    with conn.cursor() as cursor:
        
        # List and sort Parquet files
        parquet_files = [f for f in os.listdir(directory_path) if f.endswith('.parquet')]
        parquet_files.sort()

        for file in parquet_files:
            print(f"Loading {file} into PostgreSQL")
            file_path = os.path.join(directory_path, file)
            df = pd.read_parquet(file_path)

            print(df.head())

            data_list = [(row['_id'], row['title'], row['text'], np.array(row['text-embedding-3-large-1536-embedding'])) for index, row in df.iterrows()]
            # Use execute_values to perform batch insertion
            execute_values(cursor, "INSERT INTO documents (_id, title, text, embeddings) VALUES %s", data_list)
            # Commit after we insert all embeddings
            conn.commit()

            print(f"Loaded {file} into PostgreSQL")
