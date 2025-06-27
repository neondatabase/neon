use super::*;

#[test]
fn test_node_metadata_v1_backward_compatibilty() {
    let v1 = serde_json::to_vec(&serde_json::json!({
        "host": "localhost",
        "port": 23,
        "http_host": "localhost",
        "http_port": 42,
    }));

    assert_eq!(
        serde_json::from_slice::<NodeMetadata>(&v1.unwrap()).unwrap(),
        NodeMetadata {
            postgres_host: "localhost".to_string(),
            postgres_port: 23,
            grpc_host: None,
            grpc_port: None,
            http_host: "localhost".to_string(),
            http_port: 42,
            https_port: None,
            other: HashMap::new(),
        }
    )
}

#[test]
fn test_node_metadata_v2_backward_compatibilty() {
    let v2 = serde_json::to_vec(&serde_json::json!({
        "host": "localhost",
        "port": 23,
        "http_host": "localhost",
        "http_port": 42,
        "https_port": 123,
    }));

    assert_eq!(
        serde_json::from_slice::<NodeMetadata>(&v2.unwrap()).unwrap(),
        NodeMetadata {
            postgres_host: "localhost".to_string(),
            postgres_port: 23,
            grpc_host: None,
            grpc_port: None,
            http_host: "localhost".to_string(),
            http_port: 42,
            https_port: Some(123),
            other: HashMap::new(),
        }
    )
}

#[test]
fn test_node_metadata_v3_backward_compatibilty() {
    let v3 = serde_json::to_vec(&serde_json::json!({
        "host": "localhost",
        "port": 23,
        "grpc_host": "localhost",
        "grpc_port": 51,
        "http_host": "localhost",
        "http_port": 42,
        "https_port": 123,
    }));

    assert_eq!(
        serde_json::from_slice::<NodeMetadata>(&v3.unwrap()).unwrap(),
        NodeMetadata {
            postgres_host: "localhost".to_string(),
            postgres_port: 23,
            grpc_host: Some("localhost".to_string()),
            grpc_port: Some(51),
            http_host: "localhost".to_string(),
            http_port: 42,
            https_port: Some(123),
            other: HashMap::new(),
        }
    )
}
