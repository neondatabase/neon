#[cfg(test)]
mod pg_helpers_tests {

    use std::fs::File;

    use compute_tools::pg_helpers::*;
    use compute_tools::spec::ComputeSpec;

    #[test]
    fn params_serialize() {
        let file = File::open("tests/cluster_spec.json").unwrap();
        let spec: ComputeSpec = serde_json::from_reader(file).unwrap();

        assert_eq!(
            spec.cluster.databases.first().unwrap().to_pg_options(),
            "LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0 OWNER \"alexk\""
        );
        assert_eq!(
            spec.cluster.roles.first().unwrap().to_pg_options(),
            "LOGIN PASSWORD 'md56b1d16b78004bbd51fa06af9eda75972'"
        );
    }

    #[test]
    fn settings_serialize() {
        let file = File::open("tests/cluster_spec.json").unwrap();
        let spec: ComputeSpec = serde_json::from_reader(file).unwrap();

        assert_eq!(
            spec.cluster.settings.as_pg_settings(),
            "fsync = off\nwal_level = replica\nhot_standby = on\nwal_acceptors = '127.0.0.1:6502,127.0.0.1:6503,127.0.0.1:6501'\nwal_log_hints = on\nlog_connections = on\nshared_buffers = 32768\nport = 55432\nmax_connections = 100\nmax_wal_senders = 10\nlisten_addresses = '0.0.0.0'\nwal_sender_timeout = 0\npassword_encryption = md5\nmaintenance_work_mem = 65536\nmax_parallel_workers = 8\nmax_worker_processes = 8\nneon.tenantid = 'b0554b632bd4d547a63b86c3630317e8'\nmax_replication_slots = 10\nneon.timelineid = '2414a61ffc94e428f14b5758fe308e13'\nshared_preload_libraries = 'neon'\nsynchronous_standby_names = 'walproposer'\nneon.pageserver_connstring = 'host=127.0.0.1 port=6400'"
        );
    }

    #[test]
    fn quote_ident() {
        let ident: PgIdent = PgIdent::from("\"name\";\\n select 1;");

        assert_eq!(ident.quote(), "\"\"\"name\"\";\\n select 1;\"");
    }
}
