#! /usr/bin/env node

import {Connection} from 'postgresql-client';

const params = {
    "host": process.env.NEON_HOST,
    "database": process.env.NEON_DATABASE,
    "user": process.env.NEON_USER,
    "password": process.env.NEON_PASSWORD,
    "ssl": true,
}
for (const key in params) {
    if (params[key] === undefined) {
        delete params[key];
    }
}

const connection = new Connection(params);
await connection.connect();
const result = await connection.query(
    'select 1'
);
const rows = result.rows;
await connection.close();
console.log(rows[0][0]);
