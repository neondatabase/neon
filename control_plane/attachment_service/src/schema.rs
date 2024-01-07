// @generated automatically by Diesel CLI.

diesel::table! {
    nodes (node_id) {
        node_id -> BigInt,
        scheduling_policy -> Text,
        listen_http_addr -> Text,
        listen_http_port -> Integer,
        listen_pg_addr -> Text,
        listen_pg_port -> Integer,
    }
}

diesel::table! {
    tenant_shards (id) {
        id -> Integer,
        tenant_id -> Text,
        shard_number -> Integer,
        shard_count -> Integer,
        shard_stripe_size -> Integer,
        generation -> Integer,
        placement_policy -> Text,
        config -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(nodes, tenant_shards,);
