//! Neon failpoint library - a replacement for the `fail` crate with async support and context matching.
//!
//! This library provides failpoint functionality for testing with the following features:
//! - Async variants that don't require spawn_blocking
//! - Context-specific failpoints with regex matching
//! - Support for all actions used in the codebase: pause, sleep, return, exit, off
//! - Compatible API with the existing fail crate usage

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

pub mod macros;

/// Global failpoint registry
static FAILPOINTS: Lazy<Arc<RwLock<HashMap<String, FailpointConfig>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Configuration for a single failpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailpointConfig {
    /// The action to take when the failpoint is hit
    pub action: FailpointAction,
    /// Optional context matching rules
    pub context_matchers: Option<HashMap<String, String>>,
}

/// Actions that can be taken when a failpoint is hit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailpointAction {
    /// Do nothing - effectively disables the failpoint
    Off,
    /// Pause indefinitely until the failpoint is disabled
    Pause,
    /// Sleep for a specified duration in milliseconds
    Sleep(u64),
    /// Return a value (for failpoints that support it)
    Return(String),
    /// Exit the process immediately
    Exit,
}

/// Context information passed to failpoints
pub type FailpointContext = HashMap<String, String>;

/// Result of hitting a failpoint
#[derive(Debug)]
pub enum FailpointResult {
    /// Continue normal execution
    Continue,
    /// Return early with a value
    Return(String),
    /// Cancelled by cancellation token
    Cancelled,
}

/// Initialize failpoints from environment variable
pub fn init() -> Result<()> {
    if let Ok(env_failpoints) = std::env::var("FAILPOINTS") {
        for entry in env_failpoints.split(';') {
            if let Some((name, actions)) = entry.split_once('=') {
                configure_failpoint(name, actions)?;
            }
        }
    }
    Ok(())
}

/// Configure a failpoint with the given action string
pub fn configure_failpoint(name: &str, actions: &str) -> Result<()> {
    let action = parse_action(actions)?;
    let config = FailpointConfig {
        action,
        context_matchers: None,
    };
    
    let mut failpoints = FAILPOINTS.write();
    failpoints.insert(name.to_string(), config);
    
    tracing::info!("Configured failpoint: {} = {}", name, actions);
    Ok(())
}

/// Configure a failpoint with context matching
pub fn configure_failpoint_with_context(
    name: &str,
    actions: &str,
    context_matchers: HashMap<String, String>,
) -> Result<()> {
    let action = parse_action(actions)?;
    let config = FailpointConfig {
        action,
        context_matchers: Some(context_matchers),
    };
    
    let mut failpoints = FAILPOINTS.write();
    failpoints.insert(name.to_string(), config);
    
    tracing::info!("Configured failpoint with context: {} = {}", name, actions);
    Ok(())
}

/// Remove a failpoint configuration
pub fn remove_failpoint(name: &str) {
    let mut failpoints = FAILPOINTS.write();
    failpoints.remove(name);
    tracing::info!("Removed failpoint: {}", name);
}

/// Check if failpoints are enabled (for compatibility with fail crate)
pub fn has_failpoints() -> bool {
    cfg!(feature = "testing") || std::env::var("FAILPOINTS").is_ok()
}

/// Execute a failpoint with optional context
pub async fn failpoint(name: &str, context: Option<&FailpointContext>) -> FailpointResult {
    failpoint_with_cancellation(name, context, &CancellationToken::new()).await
}

/// Execute a failpoint with cancellation support
pub async fn failpoint_with_cancellation(
    name: &str,
    context: Option<&FailpointContext>,
    cancel_token: &CancellationToken,
) -> FailpointResult {
    // Only check failpoints if testing feature is enabled
    if !cfg!(feature = "testing") {
        return FailpointResult::Continue;
    }

    let config = {
        let failpoints = FAILPOINTS.read();
        failpoints.get(name).cloned()
    };

    let Some(config) = config else {
        return FailpointResult::Continue;
    };

    // Check context matchers if provided
    if let (Some(matchers), Some(ctx)) = (&config.context_matchers, context) {
        if !matches_context(matchers, ctx) {
            return FailpointResult::Continue;
        }
    }

    tracing::info!("Hit failpoint: {}", name);

    match config.action {
        FailpointAction::Off => FailpointResult::Continue,
        FailpointAction::Pause => {
            tracing::info!("Failpoint {} pausing", name);
            cancel_token.cancelled().await;
            FailpointResult::Cancelled
        }
        FailpointAction::Sleep(millis) => {
            let duration = Duration::from_millis(millis);
            tracing::info!("Failpoint {} sleeping for {:?}", name, duration);
            
            tokio::select! {
                _ = tokio::time::sleep(duration) => {
                    tracing::info!("Failpoint {} sleep completed", name);
                    FailpointResult::Continue
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("Failpoint {} sleep cancelled", name);
                    FailpointResult::Cancelled
                }
            }
        }
        FailpointAction::Return(value) => {
            tracing::info!("Failpoint {} returning: {}", name, value);
            FailpointResult::Return(value)
        }
        FailpointAction::Exit => {
            tracing::info!("Failpoint {} exiting process", name);
            std::process::exit(1);
        }
    }
}

/// Parse an action string into a FailpointAction
fn parse_action(actions: &str) -> Result<FailpointAction> {
    match actions {
        "off" => Ok(FailpointAction::Off),
        "pause" => Ok(FailpointAction::Pause),
        "exit" => Ok(FailpointAction::Exit),
        "return" => Ok(FailpointAction::Return(String::new())),
        _ => {
            // Try to parse return(value) format
            if let Some(captures) = regex::Regex::new(r"^return\(([^)]*)\)$")?.captures(actions) {
                let value = captures.get(1).unwrap().as_str().to_string();
                Ok(FailpointAction::Return(value))
            }
            // Try to parse sleep(millis) format
            else if let Some(captures) = regex::Regex::new(r"^sleep\((\d+)\)$")?.captures(actions) {
                let millis = captures.get(1).unwrap().as_str().parse::<u64>()?;
                Ok(FailpointAction::Sleep(millis))
            }
            // For backward compatibility, treat numeric values as sleep duration
            else if let Ok(millis) = actions.parse::<u64>() {
                Ok(FailpointAction::Sleep(millis))
            }
            else {
                anyhow::bail!("Invalid failpoint action: {}", actions);
            }
        }
    }
}

/// Check if the given context matches the matchers
fn matches_context(matchers: &HashMap<String, String>, context: &FailpointContext) -> bool {
    for (key, pattern) in matchers {
        let Some(value) = context.get(key) else {
            return false;
        };
        
        // Try to compile and match as regex
        if let Ok(regex) = Regex::new(pattern) {
            if !regex.is_match(value) {
                return false;
            }
        } else {
            // Fall back to exact string match
            if value != pattern {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_failpoint_off() {
        configure_failpoint("test_off", "off").unwrap();
        let result = failpoint("test_off", None).await;
        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_failpoint_return() {
        configure_failpoint("test_return", "return(42)").unwrap();
        let result = failpoint("test_return", None).await;
        if let FailpointResult::Return(value) = result {
            assert_eq!(value, "42");
        } else {
            panic!("Expected return result");
        }
    }

    #[tokio::test]
    async fn test_failpoint_sleep() {
        configure_failpoint("test_sleep", "sleep(10)").unwrap();
        let start = std::time::Instant::now();
        let result = failpoint("test_sleep", None).await;
        let duration = start.elapsed();
        
        matches!(result, FailpointResult::Continue);
        assert!(duration >= Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_failpoint_pause_with_cancellation() {
        configure_failpoint("test_pause", "pause").unwrap();
        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        
        // Cancel after 10ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel_token_clone.cancel();
        });
        
        let result = failpoint_with_cancellation("test_pause", None, &cancel_token).await;
        matches!(result, FailpointResult::Cancelled);
    }

    #[tokio::test]
    async fn test_context_matching() {
        let mut context_matchers = HashMap::new();
        context_matchers.insert("tenant_id".to_string(), "test_.*".to_string());
        
        configure_failpoint_with_context("test_context", "return(matched)", context_matchers).unwrap();
        
        let mut context = HashMap::new();
        context.insert("tenant_id".to_string(), "test_123".to_string());
        
        let result = failpoint("test_context", Some(&context)).await;
        if let FailpointResult::Return(value) = result {
            assert_eq!(value, "matched");
        } else {
            panic!("Expected return result");
        }
        
        // Test non-matching context
        context.insert("tenant_id".to_string(), "other_123".to_string());
        let result = failpoint("test_context", Some(&context)).await;
        matches!(result, FailpointResult::Continue);
    }
} 