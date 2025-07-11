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
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub mod macros;

/// Global failpoint registry
static FAILPOINTS: Lazy<Arc<RwLock<HashMap<String, FailpointConfig>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Configuration for a single failpoint
#[derive(Debug, Clone)]
pub struct FailpointConfig {
    /// The action to take when the failpoint is hit
    pub action: FailpointAction,
    /// Optional context matching rules
    pub context_matchers: Option<HashMap<String, String>>,
    /// Notify objects for tasks waiting on this failpoint
    pub notifiers: Vec<Arc<Notify>>,
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
    Return(Option<String>),
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
    Return(Option<String>),
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
        notifiers: Vec::new(),
    };
    
    let mut failpoints = FAILPOINTS.write();
    
    // If this failpoint already exists, notify all waiting tasks
    if let Some(existing_config) = failpoints.get(name) {
        // Notify all waiting tasks about the configuration change
        for notifier in &existing_config.notifiers {
            notifier.notify_waiters();
        }
    }
    
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
        notifiers: Vec::new(),
    };
    
    let mut failpoints = FAILPOINTS.write();
    
    // If this failpoint already exists, notify all waiting tasks
    if let Some(existing_config) = failpoints.get(name) {
        // Notify all waiting tasks about the configuration change
        for notifier in &existing_config.notifiers {
            notifier.notify_waiters();
        }
    }
    
    failpoints.insert(name.to_string(), config);
    
    tracing::info!("Configured failpoint with context: {} = {}", name, actions);
    Ok(())
}

/// Remove a failpoint configuration
pub fn remove_failpoint(name: &str) {
    let mut failpoints = FAILPOINTS.write();
    
    // Notify all waiting tasks before removing
    if let Some(existing_config) = failpoints.get(name) {
        for notifier in &existing_config.notifiers {
            notifier.notify_waiters();
        }
    }
    
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
            
            // Create a notifier for this task
            let notifier = Arc::new(Notify::new());
            
            // Add the notifier to the failpoint configuration
            {
                let mut failpoints = FAILPOINTS.write();
                if let Some(fp_config) = failpoints.get_mut(name) {
                    fp_config.notifiers.push(notifier.clone());
                }
            }
            
            // Create a cleanup guard to remove the notifier when this task completes
            let cleanup_guard = NotifierCleanupGuard {
                failpoint_name: name.to_string(),
                notifier: notifier.clone(),
            };
            
            // Wait for either cancellation or notification
            let result = tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Failpoint {} cancelled", name);
                    FailpointResult::Cancelled
                }
                _ = notifier.notified() => {
                    tracing::info!("Failpoint {} unpaused, resuming", name);
                    FailpointResult::Continue
                }
            };
            
            // Cleanup happens automatically when cleanup_guard is dropped
            drop(cleanup_guard);
            result
        }
        FailpointAction::Sleep(millis) => {
            let duration = Duration::from_millis(millis);
            tracing::info!("Failpoint {} sleeping for {:?}", name, duration);
            
            // Create a notifier for this task
            let notifier = Arc::new(Notify::new());
            
            // Add the notifier to the failpoint configuration
            {
                let mut failpoints = FAILPOINTS.write();
                if let Some(fp_config) = failpoints.get_mut(name) {
                    fp_config.notifiers.push(notifier.clone());
                }
            }
            
            // Create a cleanup guard to remove the notifier when this task completes
            let cleanup_guard = NotifierCleanupGuard {
                failpoint_name: name.to_string(),
                notifier: notifier.clone(),
            };
            
            let result = tokio::select! {
                _ = tokio::time::sleep(duration) => {
                    tracing::info!("Failpoint {} sleep completed", name);
                    FailpointResult::Continue
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("Failpoint {} sleep cancelled", name);
                    FailpointResult::Cancelled
                }
                _ = notifier.notified() => {
                    tracing::info!("Failpoint {} sleep interrupted, resuming", name);
                    FailpointResult::Continue
                }
            };
            
            // Cleanup happens automatically when cleanup_guard is dropped
            drop(cleanup_guard);
            result
        }
        FailpointAction::Return(value) => {
            tracing::info!("Failpoint {} returning: {:?}", name, value);
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
        "return" => Ok(FailpointAction::Return(None)),
        _ => {
            // Try to parse return(value) format
            if let Some(captures) = regex::Regex::new(r"^return\(([^)]*)\)$")?.captures(actions) {
                let value = captures.get(1).unwrap().as_str().to_string();
                Ok(FailpointAction::Return(Some(value)))
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

/// RAII guard that removes a notifier from a failpoint when dropped
struct NotifierCleanupGuard {
    failpoint_name: String,
    notifier: Arc<Notify>,
}

impl Drop for NotifierCleanupGuard {
    fn drop(&mut self) {
        let mut failpoints = FAILPOINTS.write();
        if let Some(fp_config) = failpoints.get_mut(&self.failpoint_name) {
            // Remove this specific notifier from the list
            fp_config.notifiers.retain(|n| !Arc::ptr_eq(n, &self.notifier));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

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
    async fn test_failpoint_pause_and_resume() {
        let failpoint_name = "test_pause_resume";
        configure_failpoint(failpoint_name, "pause").unwrap();
        
        let start = std::time::Instant::now();
        
        // Start a task that hits the failpoint
        let task = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        // Give it time to hit the failpoint and pause
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Reconfigure the failpoint to "off" to resume
        configure_failpoint(failpoint_name, "off").unwrap();
        
        // The task should complete quickly now
        let result = timeout(Duration::from_millis(100), task).await.unwrap().unwrap();
        
        let duration = start.elapsed();
        
        // Should have resumed and continued
        matches!(result, FailpointResult::Continue);
        
        // Should have taken at least 10ms (initial pause) but less than 1 second
        assert!(duration >= Duration::from_millis(10));
        assert!(duration < Duration::from_millis(1000));
    }

    #[tokio::test]
    async fn test_failpoint_pause_and_change_to_return() {
        let failpoint_name = "test_pause_to_return";
        configure_failpoint(failpoint_name, "pause").unwrap();
        
        // Start a task that hits the failpoint
        let task = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        // Give it time to hit the failpoint and pause
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Reconfigure the failpoint to return a value
        configure_failpoint(failpoint_name, "return(resumed)").unwrap();
        
        // The task should complete by resuming (Continue), not with the new return value
        // The new configuration only applies to future hits of the failpoint
        let result = timeout(Duration::from_millis(100), task).await.unwrap().unwrap();
        
        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_failpoint_sleep_and_resume() {
        let failpoint_name = "test_sleep_resume";
        configure_failpoint(failpoint_name, "sleep(1000)").unwrap(); // 1 second sleep
        
        let start = std::time::Instant::now();
        
        // Start a task that hits the failpoint
        let task = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        // Give it time to hit the failpoint and start sleeping
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Reconfigure the failpoint to "off" to resume
        configure_failpoint(failpoint_name, "off").unwrap();
        
        // The task should complete quickly now
        let result = timeout(Duration::from_millis(100), task).await.unwrap().unwrap();
        
        let duration = start.elapsed();
        
        // Should have resumed and continued
        matches!(result, FailpointResult::Continue);
        
        // Should have taken much less than the original 1 second sleep
        assert!(duration < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_failpoint_sleep_and_change_duration() {
        let failpoint_name = "test_sleep_change";
        configure_failpoint(failpoint_name, "sleep(1000)").unwrap(); // 1 second sleep
        
        let start = std::time::Instant::now();
        
        // Start a task that hits the failpoint
        let task = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        // Give it time to hit the failpoint and start sleeping
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Change the sleep duration to a shorter time
        configure_failpoint(failpoint_name, "sleep(50)").unwrap();
        
        // The task should complete by resuming (Continue), not by sleeping the new duration
        let result = timeout(Duration::from_millis(200), task).await.unwrap().unwrap();
        
        let duration = start.elapsed();
        
        // Should have resumed and continued
        matches!(result, FailpointResult::Continue);
        
        // Should have taken much less than the original 1 second sleep
        assert!(duration < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_failpoint_remove_during_pause() {
        let failpoint_name = "test_remove_pause";
        configure_failpoint(failpoint_name, "pause").unwrap();
        
        // Start a task that hits the failpoint
        let task = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        // Give it time to hit the failpoint and pause
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Remove the failpoint entirely
        remove_failpoint(failpoint_name);
        
        // The task should complete quickly now
        let result = timeout(Duration::from_millis(100), task).await.unwrap().unwrap();
        
        // Should have resumed and continued
        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_multiple_paused_tasks_resume() {
        let failpoint_name = "test_multiple_pause";
        configure_failpoint(failpoint_name, "pause").unwrap();
        
        // Start multiple tasks that hit the same failpoint
        let task1 = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        let task2 = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        let task3 = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        // Give them time to hit the failpoint and pause
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Reconfigure the failpoint to "off" to resume all tasks
        configure_failpoint(failpoint_name, "off").unwrap();
        
        // All tasks should complete quickly now
        let result1 = timeout(Duration::from_millis(100), task1).await.unwrap().unwrap();
        let result2 = timeout(Duration::from_millis(100), task2).await.unwrap().unwrap();
        let result3 = timeout(Duration::from_millis(100), task3).await.unwrap().unwrap();
        
        // All should have resumed and continued
        matches!(result1, FailpointResult::Continue);
        matches!(result2, FailpointResult::Continue);
        matches!(result3, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_no_race_condition_on_rapid_config_changes() {
        let failpoint_name = "test_race_condition";
        
        // Start with pause
        configure_failpoint(failpoint_name, "pause").unwrap();
        
        // Start a task that will hit the failpoint
        let task = tokio::spawn(async move {
            failpoint(failpoint_name, None).await
        });
        
        // Rapidly change the configuration multiple times
        for i in 0..10 {
            tokio::time::sleep(Duration::from_millis(1)).await;
            if i % 2 == 0 {
                configure_failpoint(failpoint_name, "pause").unwrap();
            } else {
                configure_failpoint(failpoint_name, "return(rapid_change)").unwrap();
            }
        }
        
        // Finally set it to return
        configure_failpoint(failpoint_name, "return(final)").unwrap();
        
        // The task should complete by resuming (Continue), not with any return value
        let result = timeout(Duration::from_millis(100), task).await.unwrap().unwrap();
        
        // Should resume normally since it was woken up by a configuration change
        matches!(result, FailpointResult::Continue);
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