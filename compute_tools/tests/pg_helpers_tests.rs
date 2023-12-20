#[cfg(test)]
mod pg_helpers_tests {
    use std::fs::File;

    use compute_api::spec::{ComputeSpec, GenericOption, GenericOptions, PgIdent};
    use compute_tools::pg_helpers::*;

    #[test]
    fn params_serialize() {
        let file = File::open("../libs/compute_api/tests/cluster_spec.json").unwrap();
        let spec: ComputeSpec = serde_json::from_reader(file).unwrap();

        assert_eq!(
            spec.cluster.databases.first().unwrap().to_pg_options(),
            "LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0 OWNER \"alexk\""
        );
        assert_eq!(
            spec.cluster.roles.first().unwrap().to_pg_options(),
            " LOGIN PASSWORD 'md56b1d16b78004bbd51fa06af9eda75972'"
        );
    }

    #[test]
    fn settings_serialize() {
        let file = File::open("../libs/compute_api/tests/cluster_spec.json").unwrap();
        let spec: ComputeSpec = serde_json::from_reader(file).unwrap();

        assert_eq!(
            spec.cluster.settings.as_pg_settings(),
            r#"fsync = off
wal_level = logical
hot_standby = on
neon.safekeepers = '127.0.0.1:6502,127.0.0.1:6503,127.0.0.1:6501'
wal_log_hints = on
log_connections = on
shared_buffers = 32768
port = 55432
max_connections = 100
max_wal_senders = 10
listen_addresses = '0.0.0.0'
wal_sender_timeout = 0
password_encryption = md5
maintenance_work_mem = 65536
max_parallel_workers = 8
max_worker_processes = 8
neon.tenant_id = 'b0554b632bd4d547a63b86c3630317e8'
max_replication_slots = 10
neon.timeline_id = '2414a61ffc94e428f14b5758fe308e13'
shared_preload_libraries = 'neon'
synchronous_standby_names = 'walproposer'
neon.pageserver_connstring = 'host=127.0.0.1 port=6400'
test.escaping = 'here''s a backslash \\ and a quote '' and a double-quote " hooray'
"#
        );
    }

    #[test]
    fn ident_pg_quote() {
        let ident: PgIdent = PgIdent::from("\"name\";\\n select 1;");

        assert_eq!(ident.pg_quote(), "\"\"\"name\"\";\\n select 1;\"");
    }

    #[test]
    fn generic_options_search() {
        let generic_options: GenericOptions = Some(vec![
            GenericOption {
                name: "present_value".into(),
                value: Some("value".into()),
                vartype: "string".into(),
            },
            GenericOption {
                name: "missed_value".into(),
                value: None,
                vartype: "int".into(),
            },
        ]);
        assert_eq!(generic_options.find("present_value"), Some("value".into()));
        assert_eq!(generic_options.find("missed_value"), None);
        assert_eq!(generic_options.find("invalid_value"), None);

        let empty_generic_options: GenericOptions = Some(vec![]);
        assert_eq!(empty_generic_options.find("present_value"), None);
        assert_eq!(empty_generic_options.find("missed_value"), None);
        assert_eq!(empty_generic_options.find("invalid_value"), None);

        let none_generic_options: GenericOptions = None;
        assert_eq!(none_generic_options.find("present_value"), None);
        assert_eq!(none_generic_options.find("missed_value"), None);
        assert_eq!(none_generic_options.find("invalid_value"), None);
    }

    #[test]
    fn test_escape_literal() {
        assert_eq!(escape_literal("test"), "'test'");
        assert_eq!(escape_literal("test'"), "'test'''");
        assert_eq!(escape_literal("test\\'"), "E'test\\\\'''");
        assert_eq!(escape_literal("test\\'\\'"), "E'test\\\\''\\\\'''");
    }
}
