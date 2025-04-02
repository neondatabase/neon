//! Rust repr for <https://www.postgresql.org/docs/current/errcodes-appendix.html>

/// A SQLSTATE error code
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SqlState([u8; 5]);

impl SqlState {
    /// Creates a `SqlState` from its error code.
    pub fn from_code(s: &str) -> SqlState {
        let mut code = [b'0'; 5];
        if s.len() == 5 {
            code.copy_from_slice(s.as_bytes());
        }
        SqlState(code)
    }

    /// Returns the error code corresponding to the `SqlState`.
    pub fn code(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }

    // Class 08 - Connection Exception

    /// 08000
    pub const CONNECTION_EXCEPTION: SqlState = SqlState(*b"08000");

    /// 08003
    pub const CONNECTION_DOES_NOT_EXIST: SqlState = SqlState(*b"08003");

    /// 08006
    pub const CONNECTION_FAILURE: SqlState = SqlState(*b"08006");

    /// 08001
    pub const SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION: SqlState = SqlState(*b"08001");

    /// 08P01
    pub const PROTOCOL_VIOLATION: SqlState = SqlState(*b"08P01");

    // Class 22 - Data Exception

    /// 22023
    pub const INVALID_PARAMETER_VALUE: SqlState = SqlState(*b"22023");

    // Class 3D - Invalid Catalog Name

    /// 3D000
    pub const INVALID_CATALOG_NAME: SqlState = SqlState(*b"3D000");

    // Class 3F - Invalid Schema Name

    /// 3F000
    pub const INVALID_SCHEMA_NAME: SqlState = SqlState(*b"3F000");

    // Class 40 - Transaction Rollback

    /// 40001
    pub const T_R_SERIALIZATION_FAILURE: SqlState = SqlState(*b"40001");

    // Class 42 - Syntax Error or Access Rule Violation

    /// 42601
    pub const SYNTAX_ERROR: SqlState = SqlState(*b"42601");

    // Class 53 - Insufficient Resources

    /// 53200
    pub const OUT_OF_MEMORY: SqlState = SqlState(*b"53200");

    /// 53300
    pub const TOO_MANY_CONNECTIONS: SqlState = SqlState(*b"53300");

    // Class 57 - Operator Intervention

    /// 57014
    pub const QUERY_CANCELED: SqlState = SqlState(*b"57014");
}

#[cfg(test)]
mod tests {
    use super::SqlState;

    #[test]
    fn round_trip() {
        let state = SqlState::from_code("08P01");
        assert_eq!(state, SqlState::PROTOCOL_VIOLATION);
        assert_eq!(state.code(), "08P01");
    }
}
