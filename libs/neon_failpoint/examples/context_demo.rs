use neon_failpoint::{configure_failpoint_with_context, failpoint, failpoint_context, FailpointResult};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    // Initialize tracing for better output
    tracing_subscriber::fmt::init();

    // Set up a context-specific failpoint
    let mut context_matchers = HashMap::new();
    context_matchers.insert("tenant_id".to_string(), "test_.*".to_string());
    context_matchers.insert("operation".to_string(), "backup".to_string());

    configure_failpoint_with_context(
        "backup_operation", 
        "return(simulated_failure)", 
        context_matchers
    ).unwrap();

    // Test with matching context
    let context = failpoint_context! {
        "tenant_id" => "test_123",
        "operation" => "backup",
    };

    println!("Testing with matching context...");
    match failpoint("backup_operation", Some(&context)).await {
        FailpointResult::Return(value) => {
            println!("Failpoint triggered with value: {:?}", value);
        }
        FailpointResult::Continue => {
            println!("Failpoint not triggered");
        }
        FailpointResult::Cancelled => {
            println!("Failpoint cancelled");
        }
    }

    // Test with non-matching context
    let context = failpoint_context! {
        "tenant_id" => "prod_456",
        "operation" => "backup",
    };

    println!("Testing with non-matching context...");
    match failpoint("backup_operation", Some(&context)).await {
        FailpointResult::Return(value) => {
            println!("Failpoint triggered with value: {:?}", value);
        }
        FailpointResult::Continue => {
            println!("Failpoint not triggered (expected)");
        }
        FailpointResult::Cancelled => {
            println!("Failpoint cancelled");
        }
    }
} 