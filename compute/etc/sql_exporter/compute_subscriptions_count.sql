SELECT subenabled::text AS enabled, count(*) AS subscriptions_count FROM pg_subscription GROUP BY subenabled;
