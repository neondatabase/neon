import { Client, neonConfig } from '@neondatabase/serverless'

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

(async () => {
    try {
        neonConfig.wsProxy = (host, _port) => host + ':7500/v2'
        neonConfig.pipelineConnect = "password";

        const client = new Client({
            connectionString: 'postgres://neon:test@pg.localtest.me/postgres',
        });
        client.connect();
        const { rows: [{ now }] } = await client.query('SELECT now()');
        console.log(now);
        console.log("==================================");
        console.log("WSS proxy success!");
        console.log("==================================");
        process.exit(0);
    } catch (e) {
        console.log(e);
        console.log("==================================");
        console.log("WSS proxy failed!");
        console.log("==================================");
        process.exit(1);
    }
})();