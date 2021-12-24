#[cfg(test)]
mod config_tests {

    use std::fs::{remove_file, File};
    use std::io::{Read, Write};
    use std::path::Path;

    use compute_tools::config::*;

    fn write_test_file(path: &Path, content: &str) {
        let mut file = File::create(path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
    }

    fn check_file_content(path: &Path, expected_content: &str) {
        let mut file = File::open(path).unwrap();
        let mut content = String::new();

        file.read_to_string(&mut content).unwrap();
        assert_eq!(content, expected_content);
    }

    #[test]
    fn test_line_in_file() {
        let path = Path::new("./tests/tmp/config_test.txt");
        write_test_file(path, "line1\nline2.1\t line2.2\nline3");

        let line = "line2.1\t line2.2";
        let result = line_in_file(path, line).unwrap();
        assert!(!result);
        check_file_content(path, "line1\nline2.1\t line2.2\nline3");

        let line = "line4";
        let result = line_in_file(path, line).unwrap();
        assert!(result);
        check_file_content(path, "line1\nline2.1\t line2.2\nline3\nline4");

        remove_file(path).unwrap();

        let path = Path::new("./tests/tmp/new_config_test.txt");
        let line = "line4";
        let result = line_in_file(path, line).unwrap();
        assert!(result);
        check_file_content(path, "line4");

        remove_file(path).unwrap();
    }
}
