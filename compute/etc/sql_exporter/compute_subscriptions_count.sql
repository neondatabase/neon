SELECT subenabled::pg_catalog.text AS enabled, pg_catalog.count(*) AS subscriptions_count FROM pg_catalog.pg_subscription GROUP BY subenabled;
