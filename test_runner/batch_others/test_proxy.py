import psycopg2
import json
import subprocess
import asyncio
import time


def test_proxy_password_auth(vanilla_pg, zenith_proxy):
    vanilla_pg.configure(['shared_buffers=1MB'])
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user postgres with password 'postgres';")

    zenith_proxy.start_static()

    conn = zenith_proxy.connect()

    print(zenith_proxy.safe_psql("select 1;")[0])
