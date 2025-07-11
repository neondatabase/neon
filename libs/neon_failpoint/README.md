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

## Context-Based Failpoint Configuration

Context allows you to create **conditional failpoints** that only trigger when specific runtime conditions are met. This is particularly useful for testing scenarios where you want to inject failures only for specific tenants, operations, or other contextual conditions.

### Configuring Context-Based Failpoints

Use `configure_failpoint_with_context()` to set up failpoints with context matching:

```rust
use neon_failpoint::configure_failpoint_with_context;
use std::collections::HashMap;

let mut context_matchers = HashMap::new();
context_matchers.insert("tenant_id".to_string(), "test_.*".to_string());
context_matchers.insert("operation".to_string(), "backup".to_string());

configure_failpoint_with_context(
    "backup_operation",           // failpoint name
    "return(simulated_failure)",  // action to take
    context_matchers             // context matching rules
).unwrap();
```

### Context Matching Rules

The context matching system works as follows:

1. **Key-Value Matching**: Each entry in `context_matchers` specifies a key that must exist in the runtime context
2. **Regex Support**: Values in `context_matchers` are treated as regular expressions first
3. **Fallback to Exact Match**: If the regex compilation fails, it falls back to exact string matching
4. **ALL Must Match**: All context matchers must match for the failpoint to trigger

### Runtime Context Usage

When code hits a failpoint, it provides context using the `failpoint_context!` macro:

```rust
use neon_failpoint::{failpoint, failpoint_context, FailpointResult};

let context = failpoint_context! {
    "tenant_id" => "test_123",
    "operation" => "backup",
    "user_id" => "user_456",
};

match failpoint("backup_operation", Some(&context)).await {
    FailpointResult::Return(value) => {
        // This will only trigger if ALL context matchers match
        println!("Backup failed: {}", value);
    }
    FailpointResult::Continue => {
        // Continue with normal backup operation
    }
    FailpointResult::Cancelled => {}
}
```

### Context Matching Examples

#### Regex Matching
```rust
// Configure to match test tenants only
let mut matchers = HashMap::new();
matchers.insert("tenant_id".to_string(), "test_.*".to_string());

configure_failpoint_with_context("test_failpoint", "pause", matchers).unwrap();

// This will match
let context = failpoint_context! { "tenant_id" => "test_123" };
// This will NOT match  
let context = failpoint_context! { "tenant_id" => "prod_123" };
```

#### Multiple Conditions
```rust
// Must match BOTH tenant pattern AND operation
let mut matchers = HashMap::new();
matchers.insert("tenant_id".to_string(), "test_.*".to_string());
matchers.insert("operation".to_string(), "backup".to_string());

configure_failpoint_with_context("backup_test", "return(failed)", matchers).unwrap();

// This will match (both conditions met)
let context = failpoint_context! {
    "tenant_id" => "test_123",
    "operation" => "backup",
};

// This will NOT match (missing operation)
let context = failpoint_context! {
    "tenant_id" => "test_123",
    "operation" => "restore",
};
```

#### Exact String Matching
```rust
// If regex compilation fails, falls back to exact match
let mut matchers = HashMap::new();
matchers.insert("env".to_string(), "staging".to_string());

configure_failpoint_with_context("env_specific", "sleep(1000)", matchers).unwrap();

// This will match
let context = failpoint_context! { "env" => "staging" };
// This will NOT match
let context = failpoint_context! { "env" => "production" };
```

### Benefits of Context-Based Failpoints

1. **Selective Testing**: Only inject failures for specific tenants, environments, or operations
2. **Production Safety**: Avoid accidentally triggering failpoints in production by using context filters
3. **Complex Scenarios**: Test interactions between different components with targeted failures
4. **Debugging**: Isolate issues to specific contexts without affecting the entire system

### Context vs. Non-Context Failpoints

- **Without context**: `configure_failpoint("name", "action")` - triggers for ALL hits
- **With context**: `configure_failpoint_with_context("name", "action", matchers)` - triggers only when context matches

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

### `fail_point!` - Basic Failpoint Macro

The `fail_point!` macro has three variants:

1. **Simple failpoint** - `fail_point!(name)`
   - Just checks the failpoint and continues or returns early (no value)
   - Panics if the failpoint is configured with `return(value)` since no closure is provided

2. **Failpoint with return handler** - `fail_point!(name, closure)`
   - Provides a closure to handle return values from the failpoint
   - The closure receives `Option<String>` and should return the appropriate value

3. **Conditional failpoint** - `fail_point!(name, condition, closure)`
   - Only checks the failpoint if the condition is true
   - Provides a closure to handle return values (receives `&str`)

```rust
use neon_failpoint::fail_point;

// Simple failpoint - just continue or return early
fail_point!("my_failpoint");

// Failpoint with return value handling
fail_point!("my_failpoint", |value: Option<String>| {
    match value {
        Some(v) => {
            println!("Got value: {}", v);
            return Ok(v.parse().unwrap_or_default());
        }
        None => return Ok(42), // Default return value
    }
});

// Conditional failpoint - only check if condition is met
let should_fail = some_condition();
fail_point!("conditional_failpoint", should_fail, |value: &str| {
    println!("Conditional failpoint triggered with: {}", value);
    return Err(anyhow::anyhow!("Simulated failure"));
});
```

### `fail_point_with_context!` - Context-Aware Failpoint Macro

The `fail_point_with_context!` macro has three variants that mirror `fail_point!` but include context:

1. **Simple with context** - `fail_point_with_context!(name, context)`
2. **With context and return handler** - `fail_point_with_context!(name, context, closure)`
3. **Conditional with context** - `fail_point_with_context!(name, context, condition, closure)`

```rust
use neon_failpoint::{fail_point_with_context, failpoint_context};

let context = failpoint_context! {
    "tenant_id" => "test_123",
    "operation" => "backup",
};

// Simple context failpoint
fail_point_with_context!("backup_failpoint", &context);

// Context failpoint with return handler
fail_point_with_context!("backup_failpoint", &context, |value: Option<String>| {
    match value {
        Some(v) => return Err(anyhow::anyhow!("Backup failed: {}", v)),
        None => return Err(anyhow::anyhow!("Backup failed")),
    }
});

// Conditional context failpoint
let is_test_tenant = tenant_id.starts_with("test_");
fail_point_with_context!("backup_failpoint", &context, is_test_tenant, |value: Option<String>| {
    // Only triggers for test tenants
    return Err(anyhow::anyhow!("Test tenant backup failure"));
});
```

### Other Utility Macros

```rust
use neon_failpoint::{pausable_failpoint, sleep_millis_async, failpoint_return, failpoint_bail};

// Pausable failpoint with cancellation
let cancel_token = CancellationToken::new();
if let Err(()) = pausable_failpoint!("pause_here", &cancel_token).await {
    println!("Failpoint was cancelled");
}

// Sleep failpoint
sleep_millis_async!("sleep_here", &cancel_token).await;

// Simple return failpoint - automatically parses and returns the value
failpoint_return!("return_early");

// Failpoint that bails with an error
failpoint_bail!("error_point", "Something went wrong");

// Context creation helper
let context = failpoint_context! {
    "key1" => "value1",
    "key2" => "value2",
};
```

### Argument Reference

- **`name`**: String literal - the name of the failpoint
- **`context`**: Expression that evaluates to `&HashMap<String, String>` - context for matching
- **`condition`**: Boolean expression - only check failpoint if true
- **`closure`**: Closure that handles return values:
  - For `fail_point!` with closure: receives `Option<String>`
  - For conditional variants: receives `&str`
  - For `fail_point_with_context!` with closure: receives `Option<String>`
- **`cancel`**: `&CancellationToken` - for cancellation support

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