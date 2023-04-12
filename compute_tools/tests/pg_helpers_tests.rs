#[cfg(test)]
mod pg_helpers_tests {
    use anyhow::Result;
    use compute_api::spec::{ComputeSpecV2, GenericOption, GenericOptions, PgIdent};
    use compute_tools::pg_helpers::*;

    #[test]
    fn params_serialize() -> Result<()> {
        let spec_v1_str =
            std::fs::read_to_string("../libs/compute_api/tests/spec-v1.json").unwrap();
        let spec = ComputeSpecV2::parse_and_upgrade(&spec_v1_str)?;

        assert_eq!(
            spec.databases.first().unwrap().to_pg_options(),
            "LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0 OWNER \"alexk\""
        );
        assert_eq!(
            spec.roles.first().unwrap().to_pg_options(),
            "LOGIN PASSWORD 'md56b1d16b78004bbd51fa06af9eda75972'"
        );
        Ok(())
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
}
