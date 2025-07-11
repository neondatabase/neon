# Neon Failpoint Library

A modern, async-first failpoint library for Neon, replacing the `fail` crate with enhanced functionality.

## Features

- **Async-first**: All failpoint operations are async and don't require `spawn_blocking`
- **Context matching**: Failpoints can be configured to trigger only when specific context conditions are met
- **Regex support**: Context values can be matched using regular expressions
- **Cancellation support**: All operations support cancellation tokens
- **Dynamic reconfiguration**: Paused and sleeping tasks automatically resume when failpoint configurations change
- **Backward compatibility**: Drop-in replacement for existing `fail` crate usage

## Supported Actions

- `off` - Disable the failpoint
- `pause` - Pause indefinitely until disabled, reconfigured, or cancelled
- `sleep(N)` - Sleep for N milliseconds (can be interrupted by reconfiguration)
- `return` - Return early (empty value)
- `return(value)` - Return early with a specific value
- `exit` - Exit the process immediately

## Dynamic Behavior

When a failpoint is reconfigured while tasks are waiting on it:

- **Paused tasks** will immediately resume and continue normal execution
- **Sleeping tasks** will wake up early and continue normal execution  
- **Removed failpoints** will cause all waiting tasks to resume normally

The new configuration only applies to future hits of the failpoint, not to tasks that are already waiting. This allows for flexible testing scenarios where you can pause execution, inspect state, and then resume execution dynamically.

## Example: Dynamic Reconfiguration

```rust
use neon_failpoint::{configure_failpoint, failpoint, FailpointResult};
use tokio::time::Duration;

// Start a task that will hit a failpoint
let task = tokio::spawn(async {
    println!("About to hit failpoint");
    match failpoint("test_pause", None).await {
        FailpointResult::Return(value) => println!("Returned: {}", value),
        FailpointResult::Continue => println!("Continued normally"),
        FailpointResult::Cancelled => println!("Cancelled"),
    }
});

// Configure the failpoint to pause
configure_failpoint("test_pause", "pause").unwrap();

// Let the task hit the failpoint and pause
tokio::time::sleep(Duration::from_millis(10)).await;

// Change the failpoint configuration - this will wake up the paused task
// The task will resume and continue normally (not apply the new config)
configure_failpoint("test_pause", "return(not_applied)").unwrap();

// The task will complete with Continue, not Return
let result = task.await.unwrap();
```

## Basic Usage

```rust
use neon_failpoint::{configure_failpoint, failpoint, FailpointResult};

// Configure a failpoint
configure_failpoint("my_failpoint", "return(42)").unwrap();

// Use the failpoint
match failpoint("my_failpoint", None).await {
    FailpointResult::Return(value) => {
        println!("Failpoint returned: {}", value);
        return value.parse().unwrap_or_default();
    }
    FailpointResult::Continue => {
        // Continue normal execution
    }
    FailpointResult::Cancelled => {
        // Handle cancellation
    }
}
```

## Context-Specific Failpoints

```rust
use neon_failpoint::{configure_failpoint_with_context, failpoint, failpoint_context};
use std::collections::HashMap;

// Configure a failpoint that only triggers for specific tenants
let mut context_matchers = HashMap::new();
context_matchers.insert("tenant_id".to_string(), "test_.*".to_string());
context_matchers.insert("operation".to_string(), "backup".to_string());

configure_failpoint_with_context(
    "backup_operation", 
    "return(simulated_failure)", 
    context_matchers
).unwrap();

// Use with context
let context = failpoint_context! {
    "tenant_id" => "test_123",
    "operation" => "backup",
};

match failpoint("backup_operation", Some(&context)).await {
    FailpointResult::Return(value) => {
        // This will trigger for tenant_id matching "test_.*"
        println!("Backup failed: {}", value);
    }
    FailpointResult::Continue => {
        // Continue with backup
    }
    FailpointResult::Cancelled => {}
}
```

## Macros

The library provides convenient macros for common patterns:

```rust
use neon_failpoint::{fail_point, pausable_failpoint, sleep_millis_async};

// Simple failpoint (equivalent to fail::fail_point!)
fail_point!("my_failpoint");

// Failpoint with return value handling
fail_point!("my_failpoint", |value| {
    println!("Got value: {}", value);
    return Ok(value.parse().unwrap_or_default());
});

// Pausable failpoint with cancellation
let cancel_token = CancellationToken::new();
if let Err(()) = pausable_failpoint!("pause_here", &cancel_token).await {
    println!("Failpoint was cancelled");
}

// Sleep failpoint
sleep_millis_async!("sleep_here", &cancel_token).await;
```

## Migration from `fail` crate

The library provides a compatibility layer in `libs/utils/src/failpoint_support.rs`. Most existing code should work without changes, but you can migrate to the new async APIs for better performance:

### Before (with `fail` crate):
```rust
use utils::failpoint_support::pausable_failpoint;

// This used spawn_blocking internally
pausable_failpoint!("my_failpoint", &cancel_token).await?;
```

### After (with `neon_failpoint`):
```rust
use neon_failpoint::{failpoint_with_cancellation, FailpointResult};

// This is fully async
match failpoint_with_cancellation("my_failpoint", None, &cancel_token).await {
    FailpointResult::Continue => {},
    FailpointResult::Cancelled => return Err(()),
    FailpointResult::Return(_) => {},
}
```

## Environment Variable Support

Failpoints can be configured via the `FAILPOINTS` environment variable:

```bash
FAILPOINTS="failpoint1=return(42);failpoint2=sleep(1000);failpoint3=exit"
```

## Testing

The library includes comprehensive tests and examples. Run them with:

```bash
cargo test --features testing
cargo run --example context_demo --features testing
```

## HTTP Configuration

The library integrates with the existing HTTP failpoint configuration API. Send POST requests to `/v1/failpoints` with:

```json
[
  {
    "name": "my_failpoint",
    "actions": "return(42)"
  }
]
``` 