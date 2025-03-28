/// A SQLSTATE error code
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SqlState([u8; 5]);

impl SqlState {
    /// Creates a `SqlState` from its error code.
    pub fn from_code(s: &str) -> SqlState {
        let mut code = [b'0'; 5];
        if s.len() != 5 {
            code.copy_from_slice(s.as_bytes());
        }
        SqlState(code)
    }

    /// Returns the error code corresponding to the `SqlState`.
    pub fn code(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }

    /// 00000
    pub const SUCCESSFUL_COMPLETION: SqlState = SqlState(*b"00000");

    /// 01000
    pub const WARNING: SqlState = SqlState(*b"01000");

    /// 0100C
    pub const WARNING_DYNAMIC_RESULT_SETS_RETURNED: SqlState = SqlState(*b"0100C");

    /// 01008
    pub const WARNING_IMPLICIT_ZERO_BIT_PADDING: SqlState = SqlState(*b"01008");

    /// 01003
    pub const WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION: SqlState = SqlState(*b"01003");

    /// 01007
    pub const WARNING_PRIVILEGE_NOT_GRANTED: SqlState = SqlState(*b"01007");

    /// 01006
    pub const WARNING_PRIVILEGE_NOT_REVOKED: SqlState = SqlState(*b"01006");

    /// 01004
    pub const WARNING_STRING_DATA_RIGHT_TRUNCATION: SqlState = SqlState(*b"01004");

    /// 01P01
    pub const WARNING_DEPRECATED_FEATURE: SqlState = SqlState(*b"01P01");

    /// 02000
    pub const NO_DATA: SqlState = SqlState(*b"02000");

    /// 02001
    pub const NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED: SqlState = SqlState(*b"02001");

    /// 03000
    pub const SQL_STATEMENT_NOT_YET_COMPLETE: SqlState = SqlState(*b"03000");

    /// 08000
    pub const CONNECTION_EXCEPTION: SqlState = SqlState(*b"08000");

    /// 08003
    pub const CONNECTION_DOES_NOT_EXIST: SqlState = SqlState(*b"08003");

    /// 08006
    pub const CONNECTION_FAILURE: SqlState = SqlState(*b"08006");

    /// 08001
    pub const SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION: SqlState = SqlState(*b"08001");

    /// 08004
    pub const SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION: SqlState = SqlState(*b"08004");

    /// 08007
    pub const TRANSACTION_RESOLUTION_UNKNOWN: SqlState = SqlState(*b"08007");

    /// 08P01
    pub const PROTOCOL_VIOLATION: SqlState = SqlState(*b"08P01");

    /// 09000
    pub const TRIGGERED_ACTION_EXCEPTION: SqlState = SqlState(*b"09000");

    /// 0A000
    pub const FEATURE_NOT_SUPPORTED: SqlState = SqlState(*b"0A000");

    /// 0B000
    pub const INVALID_TRANSACTION_INITIATION: SqlState = SqlState(*b"0B000");

    /// 0F000
    pub const LOCATOR_EXCEPTION: SqlState = SqlState(*b"0F000");

    /// 0F001
    pub const L_E_INVALID_SPECIFICATION: SqlState = SqlState(*b"0F001");

    /// 0L000
    pub const INVALID_GRANTOR: SqlState = SqlState(*b"0L000");

    /// 0LP01
    pub const INVALID_GRANT_OPERATION: SqlState = SqlState(*b"0LP01");

    /// 0P000
    pub const INVALID_ROLE_SPECIFICATION: SqlState = SqlState(*b"0P000");

    /// 0Z000
    pub const DIAGNOSTICS_EXCEPTION: SqlState = SqlState(*b"0Z000");

    /// 0Z002
    pub const STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER: SqlState = SqlState(*b"0Z002");

    /// 20000
    pub const CASE_NOT_FOUND: SqlState = SqlState(*b"20000");

    /// 21000
    pub const CARDINALITY_VIOLATION: SqlState = SqlState(*b"21000");

    /// 22000
    pub const DATA_EXCEPTION: SqlState = SqlState(*b"22000");

    /// 2202E
    pub const ARRAY_ELEMENT_ERROR: SqlState = SqlState(*b"2202E");

    /// 2202E
    pub const ARRAY_SUBSCRIPT_ERROR: SqlState = SqlState(*b"2202E");

    /// 22021
    pub const CHARACTER_NOT_IN_REPERTOIRE: SqlState = SqlState(*b"22021");

    /// 22008
    pub const DATETIME_FIELD_OVERFLOW: SqlState = SqlState(*b"22008");

    /// 22008
    pub const DATETIME_VALUE_OUT_OF_RANGE: SqlState = SqlState(*b"22008");

    /// 22012
    pub const DIVISION_BY_ZERO: SqlState = SqlState(*b"22012");

    /// 22005
    pub const ERROR_IN_ASSIGNMENT: SqlState = SqlState(*b"22005");

    /// 2200B
    pub const ESCAPE_CHARACTER_CONFLICT: SqlState = SqlState(*b"2200B");

    /// 22022
    pub const INDICATOR_OVERFLOW: SqlState = SqlState(*b"22022");

    /// 22015
    pub const INTERVAL_FIELD_OVERFLOW: SqlState = SqlState(*b"22015");

    /// 2201E
    pub const INVALID_ARGUMENT_FOR_LOG: SqlState = SqlState(*b"2201E");

    /// 22014
    pub const INVALID_ARGUMENT_FOR_NTILE: SqlState = SqlState(*b"22014");

    /// 22016
    pub const INVALID_ARGUMENT_FOR_NTH_VALUE: SqlState = SqlState(*b"22016");

    /// 2201F
    pub const INVALID_ARGUMENT_FOR_POWER_FUNCTION: SqlState = SqlState(*b"2201F");

    /// 2201G
    pub const INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION: SqlState = SqlState(*b"2201G");

    /// 22018
    pub const INVALID_CHARACTER_VALUE_FOR_CAST: SqlState = SqlState(*b"22018");

    /// 22007
    pub const INVALID_DATETIME_FORMAT: SqlState = SqlState(*b"22007");

    /// 22019
    pub const INVALID_ESCAPE_CHARACTER: SqlState = SqlState(*b"22019");

    /// 2200D
    pub const INVALID_ESCAPE_OCTET: SqlState = SqlState(*b"2200D");

    /// 22025
    pub const INVALID_ESCAPE_SEQUENCE: SqlState = SqlState(*b"22025");

    /// 22P06
    pub const NONSTANDARD_USE_OF_ESCAPE_CHARACTER: SqlState = SqlState(*b"22P06");

    /// 22010
    pub const INVALID_INDICATOR_PARAMETER_VALUE: SqlState = SqlState(*b"22010");

    /// 22023
    pub const INVALID_PARAMETER_VALUE: SqlState = SqlState(*b"22023");

    /// 22013
    pub const INVALID_PRECEDING_OR_FOLLOWING_SIZE: SqlState = SqlState(*b"22013");

    /// 2201B
    pub const INVALID_REGULAR_EXPRESSION: SqlState = SqlState(*b"2201B");

    /// 2201W
    pub const INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: SqlState = SqlState(*b"2201W");

    /// 2201X
    pub const INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: SqlState = SqlState(*b"2201X");

    /// 2202H
    pub const INVALID_TABLESAMPLE_ARGUMENT: SqlState = SqlState(*b"2202H");

    /// 2202G
    pub const INVALID_TABLESAMPLE_REPEAT: SqlState = SqlState(*b"2202G");

    /// 22009
    pub const INVALID_TIME_ZONE_DISPLACEMENT_VALUE: SqlState = SqlState(*b"22009");

    /// 2200C
    pub const INVALID_USE_OF_ESCAPE_CHARACTER: SqlState = SqlState(*b"2200C");

    /// 2200G
    pub const MOST_SPECIFIC_TYPE_MISMATCH: SqlState = SqlState(*b"2200G");

    /// 22004
    pub const NULL_VALUE_NOT_ALLOWED: SqlState = SqlState(*b"22004");

    /// 22002
    pub const NULL_VALUE_NO_INDICATOR_PARAMETER: SqlState = SqlState(*b"22002");

    /// 22003
    pub const NUMERIC_VALUE_OUT_OF_RANGE: SqlState = SqlState(*b"22003");

    /// 2200H
    pub const SEQUENCE_GENERATOR_LIMIT_EXCEEDED: SqlState = SqlState(*b"2200H");

    /// 22026
    pub const STRING_DATA_LENGTH_MISMATCH: SqlState = SqlState(*b"22026");

    /// 22001
    pub const STRING_DATA_RIGHT_TRUNCATION: SqlState = SqlState(*b"22001");

    /// 22011
    pub const SUBSTRING_ERROR: SqlState = SqlState(*b"22011");

    /// 22027
    pub const TRIM_ERROR: SqlState = SqlState(*b"22027");

    /// 22024
    pub const UNTERMINATED_C_STRING: SqlState = SqlState(*b"22024");

    /// 2200F
    pub const ZERO_LENGTH_CHARACTER_STRING: SqlState = SqlState(*b"2200F");

    /// 22P01
    pub const FLOATING_POINT_EXCEPTION: SqlState = SqlState(*b"22P01");

    /// 22P02
    pub const INVALID_TEXT_REPRESENTATION: SqlState = SqlState(*b"22P02");

    /// 22P03
    pub const INVALID_BINARY_REPRESENTATION: SqlState = SqlState(*b"22P03");

    /// 22P04
    pub const BAD_COPY_FILE_FORMAT: SqlState = SqlState(*b"22P04");

    /// 22P05
    pub const UNTRANSLATABLE_CHARACTER: SqlState = SqlState(*b"22P05");

    /// 2200L
    pub const NOT_AN_XML_DOCUMENT: SqlState = SqlState(*b"2200L");

    /// 2200M
    pub const INVALID_XML_DOCUMENT: SqlState = SqlState(*b"2200M");

    /// 2200N
    pub const INVALID_XML_CONTENT: SqlState = SqlState(*b"2200N");

    /// 2200S
    pub const INVALID_XML_COMMENT: SqlState = SqlState(*b"2200S");

    /// 2200T
    pub const INVALID_XML_PROCESSING_INSTRUCTION: SqlState = SqlState(*b"2200T");

    /// 22030
    pub const DUPLICATE_JSON_OBJECT_KEY_VALUE: SqlState = SqlState(*b"22030");

    /// 22031
    pub const INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION: SqlState = SqlState(*b"22031");

    /// 22032
    pub const INVALID_JSON_TEXT: SqlState = SqlState(*b"22032");

    /// 22033
    pub const INVALID_SQL_JSON_SUBSCRIPT: SqlState = SqlState(*b"22033");

    /// 22034
    pub const MORE_THAN_ONE_SQL_JSON_ITEM: SqlState = SqlState(*b"22034");

    /// 22035
    pub const NO_SQL_JSON_ITEM: SqlState = SqlState(*b"22035");

    /// 22036
    pub const NON_NUMERIC_SQL_JSON_ITEM: SqlState = SqlState(*b"22036");

    /// 22037
    pub const NON_UNIQUE_KEYS_IN_A_JSON_OBJECT: SqlState = SqlState(*b"22037");

    /// 22038
    pub const SINGLETON_SQL_JSON_ITEM_REQUIRED: SqlState = SqlState(*b"22038");

    /// 22039
    pub const SQL_JSON_ARRAY_NOT_FOUND: SqlState = SqlState(*b"22039");

    /// 2203A
    pub const SQL_JSON_MEMBER_NOT_FOUND: SqlState = SqlState(*b"2203A");

    /// 2203B
    pub const SQL_JSON_NUMBER_NOT_FOUND: SqlState = SqlState(*b"2203B");

    /// 2203C
    pub const SQL_JSON_OBJECT_NOT_FOUND: SqlState = SqlState(*b"2203C");

    /// 2203D
    pub const TOO_MANY_JSON_ARRAY_ELEMENTS: SqlState = SqlState(*b"2203D");

    /// 2203E
    pub const TOO_MANY_JSON_OBJECT_MEMBERS: SqlState = SqlState(*b"2203E");

    /// 2203F
    pub const SQL_JSON_SCALAR_REQUIRED: SqlState = SqlState(*b"2203F");

    /// 2203G
    pub const SQL_JSON_ITEM_CANNOT_BE_CAST_TO_TARGET_TYPE: SqlState = SqlState(*b"2203G");

    /// 23000
    pub const INTEGRITY_CONSTRAINT_VIOLATION: SqlState = SqlState(*b"23000");

    /// 23001
    pub const RESTRICT_VIOLATION: SqlState = SqlState(*b"23001");

    /// 23502
    pub const NOT_NULL_VIOLATION: SqlState = SqlState(*b"23502");

    /// 23503
    pub const FOREIGN_KEY_VIOLATION: SqlState = SqlState(*b"23503");

    /// 23505
    pub const UNIQUE_VIOLATION: SqlState = SqlState(*b"23505");

    /// 23514
    pub const CHECK_VIOLATION: SqlState = SqlState(*b"23514");

    /// 23P01
    pub const EXCLUSION_VIOLATION: SqlState = SqlState(*b"23P01");

    /// 24000
    pub const INVALID_CURSOR_STATE: SqlState = SqlState(*b"24000");

    /// 25000
    pub const INVALID_TRANSACTION_STATE: SqlState = SqlState(*b"25000");

    /// 25001
    pub const ACTIVE_SQL_TRANSACTION: SqlState = SqlState(*b"25001");

    /// 25002
    pub const BRANCH_TRANSACTION_ALREADY_ACTIVE: SqlState = SqlState(*b"25002");

    /// 25008
    pub const HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL: SqlState = SqlState(*b"25008");

    /// 25003
    pub const INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION: SqlState = SqlState(*b"25003");

    /// 25004
    pub const INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION: SqlState = SqlState(*b"25004");

    /// 25005
    pub const NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION: SqlState = SqlState(*b"25005");

    /// 25006
    pub const READ_ONLY_SQL_TRANSACTION: SqlState = SqlState(*b"25006");

    /// 25007
    pub const SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED: SqlState = SqlState(*b"25007");

    /// 25P01
    pub const NO_ACTIVE_SQL_TRANSACTION: SqlState = SqlState(*b"25P01");

    /// 25P02
    pub const IN_FAILED_SQL_TRANSACTION: SqlState = SqlState(*b"25P02");

    /// 25P03
    pub const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: SqlState = SqlState(*b"25P03");

    /// 26000
    pub const INVALID_SQL_STATEMENT_NAME: SqlState = SqlState(*b"26000");

    /// 26000
    pub const UNDEFINED_PSTATEMENT: SqlState = SqlState(*b"26000");

    /// 27000
    pub const TRIGGERED_DATA_CHANGE_VIOLATION: SqlState = SqlState(*b"27000");

    /// 28000
    pub const INVALID_AUTHORIZATION_SPECIFICATION: SqlState = SqlState(*b"28000");

    /// 28P01
    pub const INVALID_PASSWORD: SqlState = SqlState(*b"28P01");

    /// 2B000
    pub const DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST: SqlState = SqlState(*b"2B000");

    /// 2BP01
    pub const DEPENDENT_OBJECTS_STILL_EXIST: SqlState = SqlState(*b"2BP01");

    /// 2D000
    pub const INVALID_TRANSACTION_TERMINATION: SqlState = SqlState(*b"2D000");

    /// 2F000
    pub const SQL_ROUTINE_EXCEPTION: SqlState = SqlState(*b"2F000");

    /// 2F005
    pub const S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT: SqlState = SqlState(*b"2F005");

    /// 2F002
    pub const S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED: SqlState = SqlState(*b"2F002");

    /// 2F003
    pub const S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED: SqlState = SqlState(*b"2F003");

    /// 2F004
    pub const S_R_E_READING_SQL_DATA_NOT_PERMITTED: SqlState = SqlState(*b"2F004");

    /// 34000
    pub const INVALID_CURSOR_NAME: SqlState = SqlState(*b"34000");

    /// 34000
    pub const UNDEFINED_CURSOR: SqlState = SqlState(*b"34000");

    /// 38000
    pub const EXTERNAL_ROUTINE_EXCEPTION: SqlState = SqlState(*b"38000");

    /// 38001
    pub const E_R_E_CONTAINING_SQL_NOT_PERMITTED: SqlState = SqlState(*b"38001");

    /// 38002
    pub const E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED: SqlState = SqlState(*b"38002");

    /// 38003
    pub const E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED: SqlState = SqlState(*b"38003");

    /// 38004
    pub const E_R_E_READING_SQL_DATA_NOT_PERMITTED: SqlState = SqlState(*b"38004");

    /// 39000
    pub const EXTERNAL_ROUTINE_INVOCATION_EXCEPTION: SqlState = SqlState(*b"39000");

    /// 39001
    pub const E_R_I_E_INVALID_SQLSTATE_RETURNED: SqlState = SqlState(*b"39001");

    /// 39004
    pub const E_R_I_E_NULL_VALUE_NOT_ALLOWED: SqlState = SqlState(*b"39004");

    /// 39P01
    pub const E_R_I_E_TRIGGER_PROTOCOL_VIOLATED: SqlState = SqlState(*b"39P01");

    /// 39P02
    pub const E_R_I_E_SRF_PROTOCOL_VIOLATED: SqlState = SqlState(*b"39P02");

    /// 39P03
    pub const E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED: SqlState = SqlState(*b"39P03");

    /// 3B000
    pub const SAVEPOINT_EXCEPTION: SqlState = SqlState(*b"3B000");

    /// 3B001
    pub const S_E_INVALID_SPECIFICATION: SqlState = SqlState(*b"3B001");

    /// 3D000
    pub const INVALID_CATALOG_NAME: SqlState = SqlState(*b"3D000");

    /// 3D000
    pub const UNDEFINED_DATABASE: SqlState = SqlState(*b"3D000");

    /// 3F000
    pub const INVALID_SCHEMA_NAME: SqlState = SqlState(*b"3F000");

    /// 3F000
    pub const UNDEFINED_SCHEMA: SqlState = SqlState(*b"3F000");

    /// 40000
    pub const TRANSACTION_ROLLBACK: SqlState = SqlState(*b"40000");

    /// 40002
    pub const T_R_INTEGRITY_CONSTRAINT_VIOLATION: SqlState = SqlState(*b"40002");

    /// 40001
    pub const T_R_SERIALIZATION_FAILURE: SqlState = SqlState(*b"40001");

    /// 40003
    pub const T_R_STATEMENT_COMPLETION_UNKNOWN: SqlState = SqlState(*b"40003");

    /// 40P01
    pub const T_R_DEADLOCK_DETECTED: SqlState = SqlState(*b"40P01");

    /// 42000
    pub const SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION: SqlState = SqlState(*b"42000");

    /// 42601
    pub const SYNTAX_ERROR: SqlState = SqlState(*b"42601");

    /// 42501
    pub const INSUFFICIENT_PRIVILEGE: SqlState = SqlState(*b"42501");

    /// 42846
    pub const CANNOT_COERCE: SqlState = SqlState(*b"42846");

    /// 42803
    pub const GROUPING_ERROR: SqlState = SqlState(*b"42803");

    /// 42P20
    pub const WINDOWING_ERROR: SqlState = SqlState(*b"42P20");

    /// 42P19
    pub const INVALID_RECURSION: SqlState = SqlState(*b"42P19");

    /// 42830
    pub const INVALID_FOREIGN_KEY: SqlState = SqlState(*b"42830");

    /// 42602
    pub const INVALID_NAME: SqlState = SqlState(*b"42602");

    /// 42622
    pub const NAME_TOO_LONG: SqlState = SqlState(*b"42622");

    /// 42939
    pub const RESERVED_NAME: SqlState = SqlState(*b"42939");

    /// 42804
    pub const DATATYPE_MISMATCH: SqlState = SqlState(*b"42804");

    /// 42P18
    pub const INDETERMINATE_DATATYPE: SqlState = SqlState(*b"42P18");

    /// 42P21
    pub const COLLATION_MISMATCH: SqlState = SqlState(*b"42P21");

    /// 42P22
    pub const INDETERMINATE_COLLATION: SqlState = SqlState(*b"42P22");

    /// 42809
    pub const WRONG_OBJECT_TYPE: SqlState = SqlState(*b"42809");

    /// 428C9
    pub const GENERATED_ALWAYS: SqlState = SqlState(*b"428C9");

    /// 42703
    pub const UNDEFINED_COLUMN: SqlState = SqlState(*b"42703");

    /// 42883
    pub const UNDEFINED_FUNCTION: SqlState = SqlState(*b"42883");

    /// 42P01
    pub const UNDEFINED_TABLE: SqlState = SqlState(*b"42P01");

    /// 42P02
    pub const UNDEFINED_PARAMETER: SqlState = SqlState(*b"42P02");

    /// 42704
    pub const UNDEFINED_OBJECT: SqlState = SqlState(*b"42704");

    /// 42701
    pub const DUPLICATE_COLUMN: SqlState = SqlState(*b"42701");

    /// 42P03
    pub const DUPLICATE_CURSOR: SqlState = SqlState(*b"42P03");

    /// 42P04
    pub const DUPLICATE_DATABASE: SqlState = SqlState(*b"42P04");

    /// 42723
    pub const DUPLICATE_FUNCTION: SqlState = SqlState(*b"42723");

    /// 42P05
    pub const DUPLICATE_PSTATEMENT: SqlState = SqlState(*b"42P05");

    /// 42P06
    pub const DUPLICATE_SCHEMA: SqlState = SqlState(*b"42P06");

    /// 42P07
    pub const DUPLICATE_TABLE: SqlState = SqlState(*b"42P07");

    /// 42712
    pub const DUPLICATE_ALIAS: SqlState = SqlState(*b"42712");

    /// 42710
    pub const DUPLICATE_OBJECT: SqlState = SqlState(*b"42710");

    /// 42702
    pub const AMBIGUOUS_COLUMN: SqlState = SqlState(*b"42702");

    /// 42725
    pub const AMBIGUOUS_FUNCTION: SqlState = SqlState(*b"42725");

    /// 42P08
    pub const AMBIGUOUS_PARAMETER: SqlState = SqlState(*b"42P08");

    /// 42P09
    pub const AMBIGUOUS_ALIAS: SqlState = SqlState(*b"42P09");

    /// 42P10
    pub const INVALID_COLUMN_REFERENCE: SqlState = SqlState(*b"42P10");

    /// 42611
    pub const INVALID_COLUMN_DEFINITION: SqlState = SqlState(*b"42611");

    /// 42P11
    pub const INVALID_CURSOR_DEFINITION: SqlState = SqlState(*b"42P11");

    /// 42P12
    pub const INVALID_DATABASE_DEFINITION: SqlState = SqlState(*b"42P12");

    /// 42P13
    pub const INVALID_FUNCTION_DEFINITION: SqlState = SqlState(*b"42P13");

    /// 42P14
    pub const INVALID_PSTATEMENT_DEFINITION: SqlState = SqlState(*b"42P14");

    /// 42P15
    pub const INVALID_SCHEMA_DEFINITION: SqlState = SqlState(*b"42P15");

    /// 42P16
    pub const INVALID_TABLE_DEFINITION: SqlState = SqlState(*b"42P16");

    /// 42P17
    pub const INVALID_OBJECT_DEFINITION: SqlState = SqlState(*b"42P17");

    /// 44000
    pub const WITH_CHECK_OPTION_VIOLATION: SqlState = SqlState(*b"44000");

    /// 53000
    pub const INSUFFICIENT_RESOURCES: SqlState = SqlState(*b"53000");

    /// 53100
    pub const DISK_FULL: SqlState = SqlState(*b"53100");

    /// 53200
    pub const OUT_OF_MEMORY: SqlState = SqlState(*b"53200");

    /// 53300
    pub const TOO_MANY_CONNECTIONS: SqlState = SqlState(*b"53300");

    /// 53400
    pub const CONFIGURATION_LIMIT_EXCEEDED: SqlState = SqlState(*b"53400");

    /// 54000
    pub const PROGRAM_LIMIT_EXCEEDED: SqlState = SqlState(*b"54000");

    /// 54001
    pub const STATEMENT_TOO_COMPLEX: SqlState = SqlState(*b"54001");

    /// 54011
    pub const TOO_MANY_COLUMNS: SqlState = SqlState(*b"54011");

    /// 54023
    pub const TOO_MANY_ARGUMENTS: SqlState = SqlState(*b"54023");

    /// 55000
    pub const OBJECT_NOT_IN_PREREQUISITE_STATE: SqlState = SqlState(*b"55000");

    /// 55006
    pub const OBJECT_IN_USE: SqlState = SqlState(*b"55006");

    /// 55P02
    pub const CANT_CHANGE_RUNTIME_PARAM: SqlState = SqlState(*b"55P02");

    /// 55P03
    pub const LOCK_NOT_AVAILABLE: SqlState = SqlState(*b"55P03");

    /// 55P04
    pub const UNSAFE_NEW_ENUM_VALUE_USAGE: SqlState = SqlState(*b"55P04");

    /// 57000
    pub const OPERATOR_INTERVENTION: SqlState = SqlState(*b"57000");

    /// 57014
    pub const QUERY_CANCELED: SqlState = SqlState(*b"57014");

    /// 57P01
    pub const ADMIN_SHUTDOWN: SqlState = SqlState(*b"57P01");

    /// 57P02
    pub const CRASH_SHUTDOWN: SqlState = SqlState(*b"57P02");

    /// 57P03
    pub const CANNOT_CONNECT_NOW: SqlState = SqlState(*b"57P03");

    /// 57P04
    pub const DATABASE_DROPPED: SqlState = SqlState(*b"57P04");

    /// 57P05
    pub const IDLE_SESSION_TIMEOUT: SqlState = SqlState(*b"57P05");

    /// 58000
    pub const SYSTEM_ERROR: SqlState = SqlState(*b"58000");

    /// 58030
    pub const IO_ERROR: SqlState = SqlState(*b"58030");

    /// 58P01
    pub const UNDEFINED_FILE: SqlState = SqlState(*b"58P01");

    /// 58P02
    pub const DUPLICATE_FILE: SqlState = SqlState(*b"58P02");

    /// 72000
    pub const SNAPSHOT_TOO_OLD: SqlState = SqlState(*b"72000");

    /// F0000
    pub const CONFIG_FILE_ERROR: SqlState = SqlState(*b"F0000");

    /// F0001
    pub const LOCK_FILE_EXISTS: SqlState = SqlState(*b"F0001");

    /// HV000
    pub const FDW_ERROR: SqlState = SqlState(*b"HV000");

    /// HV005
    pub const FDW_COLUMN_NAME_NOT_FOUND: SqlState = SqlState(*b"HV005");

    /// HV002
    pub const FDW_DYNAMIC_PARAMETER_VALUE_NEEDED: SqlState = SqlState(*b"HV002");

    /// HV010
    pub const FDW_FUNCTION_SEQUENCE_ERROR: SqlState = SqlState(*b"HV010");

    /// HV021
    pub const FDW_INCONSISTENT_DESCRIPTOR_INFORMATION: SqlState = SqlState(*b"HV021");

    /// HV024
    pub const FDW_INVALID_ATTRIBUTE_VALUE: SqlState = SqlState(*b"HV024");

    /// HV007
    pub const FDW_INVALID_COLUMN_NAME: SqlState = SqlState(*b"HV007");

    /// HV008
    pub const FDW_INVALID_COLUMN_NUMBER: SqlState = SqlState(*b"HV008");

    /// HV004
    pub const FDW_INVALID_DATA_TYPE: SqlState = SqlState(*b"HV004");

    /// HV006
    pub const FDW_INVALID_DATA_TYPE_DESCRIPTORS: SqlState = SqlState(*b"HV006");

    /// HV091
    pub const FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER: SqlState = SqlState(*b"HV091");

    /// HV00B
    pub const FDW_INVALID_HANDLE: SqlState = SqlState(*b"HV00B");

    /// HV00C
    pub const FDW_INVALID_OPTION_INDEX: SqlState = SqlState(*b"HV00C");

    /// HV00D
    pub const FDW_INVALID_OPTION_NAME: SqlState = SqlState(*b"HV00D");

    /// HV090
    pub const FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH: SqlState = SqlState(*b"HV090");

    /// HV00A
    pub const FDW_INVALID_STRING_FORMAT: SqlState = SqlState(*b"HV00A");

    /// HV009
    pub const FDW_INVALID_USE_OF_NULL_POINTER: SqlState = SqlState(*b"HV009");

    /// HV014
    pub const FDW_TOO_MANY_HANDLES: SqlState = SqlState(*b"HV014");

    /// HV001
    pub const FDW_OUT_OF_MEMORY: SqlState = SqlState(*b"HV001");

    /// HV00P
    pub const FDW_NO_SCHEMAS: SqlState = SqlState(*b"HV00P");

    /// HV00J
    pub const FDW_OPTION_NAME_NOT_FOUND: SqlState = SqlState(*b"HV00J");

    /// HV00K
    pub const FDW_REPLY_HANDLE: SqlState = SqlState(*b"HV00K");

    /// HV00Q
    pub const FDW_SCHEMA_NOT_FOUND: SqlState = SqlState(*b"HV00Q");

    /// HV00R
    pub const FDW_TABLE_NOT_FOUND: SqlState = SqlState(*b"HV00R");

    /// HV00L
    pub const FDW_UNABLE_TO_CREATE_EXECUTION: SqlState = SqlState(*b"HV00L");

    /// HV00M
    pub const FDW_UNABLE_TO_CREATE_REPLY: SqlState = SqlState(*b"HV00M");

    /// HV00N
    pub const FDW_UNABLE_TO_ESTABLISH_CONNECTION: SqlState = SqlState(*b"HV00N");

    /// P0000
    pub const PLPGSQL_ERROR: SqlState = SqlState(*b"P0000");

    /// P0001
    pub const RAISE_EXCEPTION: SqlState = SqlState(*b"P0001");

    /// P0002
    pub const NO_DATA_FOUND: SqlState = SqlState(*b"P0002");

    /// P0003
    pub const TOO_MANY_ROWS: SqlState = SqlState(*b"P0003");

    /// P0004
    pub const ASSERT_FAILURE: SqlState = SqlState(*b"P0004");

    /// XX000
    pub const INTERNAL_ERROR: SqlState = SqlState(*b"XX000");

    /// XX001
    pub const DATA_CORRUPTED: SqlState = SqlState(*b"XX001");

    /// XX002
    pub const INDEX_CORRUPTED: SqlState = SqlState(*b"XX002");
}
