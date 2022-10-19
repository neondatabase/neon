#[cfg(test)]
mod pg_helpers_tests {

    use std::fs::File;

    use url::Url;

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
            "fsync = off\nwal_level = replica\nhot_standby = on\nneon.safekeepers = '127.0.0.1:6502,127.0.0.1:6503,127.0.0.1:6501'\nwal_log_hints = on\nlog_connections = on\nshared_buffers = 32768\nport = 55432\nmax_connections = 100\nmax_wal_senders = 10\nlisten_addresses = '0.0.0.0'\nwal_sender_timeout = 0\npassword_encryption = md5\nmaintenance_work_mem = 65536\nmax_parallel_workers = 8\nmax_worker_processes = 8\nneon.tenant_id = 'b0554b632bd4d547a63b86c3630317e8'\nmax_replication_slots = 10\nneon.timeline_id = '2414a61ffc94e428f14b5758fe308e13'\nshared_preload_libraries = 'neon'\nsynchronous_standby_names = 'walproposer'\nneon.pageserver_connstring = 'host=127.0.0.1 port=6400'"
        );
    }

    #[test]
    fn ident_pg_quote() {
        let ident: PgIdent = PgIdent::from("\"name\";\\n select 1;");

        assert_eq!(ident.pg_quote(), "\"\"\"name\"\";\\n select 1;\"");
    }

    fn test_one_ident_encode(url: &Url, base_url: &str, ident: &str, expected_ident: &str) {
        let ident: PgIdent = PgIdent::from(ident);
        let escaped_ident = ident.url_encode();
        assert_eq!(escaped_ident, expected_ident);

        // Also test that after setting the escaped ident as a path in the URL,
        // returned result matches our expectations.
        let expected_url = format!("{base_url}/{expected_ident}");
        let mut url = url.clone();
        url.set_path(&escaped_ident);
        assert_eq!(url.as_str(), expected_url);
    }

    #[test]
    fn ident_url_encode() {
        let base_url = "postgres://admin:asdasdasd@localhost:5432";
        let original_url = format!("{base_url}/postgres");
        let url = Url::parse(&original_url).unwrap();

        // Bellow are all valid database names that you can create in Postgres
        // and connect to via DATABASE_URL after encoding.
        test_one_ident_encode(
            &url,
            base_url,
            "mydb🤦%20\\  ",
            "mydb%F0%9F%A4%A6%2520%5C%E2%80%81%20",
        );
        test_one_ident_encode(&url, base_url, " ", "%E2%80%81");
        test_one_ident_encode(&url, base_url, " ", "%20");
        test_one_ident_encode(&url, base_url, "kinda/path/like/", "kinda%2Fpath%2Flike%2F");
        test_one_ident_encode(&url, base_url, "?", "%3F");
        test_one_ident_encode(&url, base_url, "+", "%2B");
    }
}
