#! /usr/bin/env node

import ws from 'ws'
import { neonConfig, Client } from '@neondatabase/serverless'

(async () => {
    neonConfig.webSocketConstructor = ws

    const client = new Client({
        host: process.env.NEON_HOST,
        database: process.env.NEON_DATABASE,
        user: process.env.NEON_USER,
        password: process.env.NEON_PASSWORD,
    });
    client.connect();
    const result = await client.query({
        text: 'select 1',
        rowMode: 'array',
    });
    const rows = result.rows;
    await client.end();
    console.log(rows[0][0]);
})()
