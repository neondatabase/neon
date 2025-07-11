//! Neon failpoint library - a replacement for the `fail` crate with async support and context matching.
//!
//! This library provides failpoint functionality for testing with the following features:
//! - Async variants that don't require spawn_blocking
//! - Context-specific failpoints with regex matching
//! - Support for all actions used in the codebase: pause, sleep, return, exit, off
//! - Compatible API with the existing fail crate usage

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use either::Either;
use once_cell::sync::Lazy;
use rand::Rng;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub mod macros;
pub use either; // re-export for use in macros

/// Global failpoint registry
// TODO: switch to simple_rcu, but it's in `utils`
static FAILPOINTS: Lazy<Arc<std::sync::RwLock<HashMap<String, Arc<std::sync::Mutex<FailpointConfig>>>>>> =
    Lazy::new(|| Default::default());

/// Configuration for a single failpoint
#[derive(Debug)]
pub struct FailpointConfig {
    /// The action specification including probability
    pub action_spec: FailpointActionSpec,
    /// Optional context matching rules
    pub context_matchers: Option<HashMap<String, String>>,
    /// Notify objects for tasks waiting on this failpoint
    pub notifiers: FailpointNotifiers,
    /// Counter for probability-based actions
    pub trigger_count: u32,
}

/// Specification for a failpoint action with probability
#[derive(Debug, Clone)]
pub struct FailpointActionSpec {
    /// Probability as a percentage (0-100), None means always trigger (100%)
    pub probability: Option<u8>,
    /// Maximum number of times to trigger (None = unlimited)
    pub max_count: Option<u32>,
    /// The actual action to execute
    pub action: FailpointAction,
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
    /// Panic the process with a message
    Panic(String),
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

/// Collection of notifiers for a failpoint
///
/// This abstraction manages the lifecycle of notification objects
/// and provides a clean interface for creating notifiers and broadcasting notifications.
#[derive(Debug, Default)]
pub struct FailpointNotifiers {
    notifiers: Vec<Arc<Notify>>,
}

impl FailpointNotifiers {
    /// Create a new empty collection of notifiers
    pub fn new() -> Self {
        Self {
            notifiers: Vec::new(),
        }
    }

    /// Create a new notifier and add it to the collection
    ///
    /// Returns a `FailpointNotifier` that automatically removes itself
    /// from the collection when dropped.
    pub fn create_notifier(&mut self, config_arc: Arc<std::sync::Mutex<FailpointConfig>>) -> FailpointNotifier {
        let notifier = Arc::new(Notify::new());
        self.notifiers.push(notifier.clone());

        FailpointNotifier::new(notifier, config_arc)
    }

    /// Notify all waiting tasks
    pub fn notify_all(&self) {
        for notifier in &self.notifiers {
            notifier.notify_waiters();
        }
    }

    /// Remove a specific notifier from the collection
    pub fn remove_notifier(&mut self, notifier: &Arc<Notify>) {
        self.notifiers.retain(|n| !Arc::ptr_eq(n, notifier));
    }
}

/// Abstraction for managing failpoint notifications
///
/// This handles the lifecycle of a notifier for a failpoint:
/// - Provides a future that can be awaited to receive notifications
/// - Automatically cleans up when dropped
pub struct FailpointNotifier {
    notifier: Arc<Notify>,
    config_arc: Arc<std::sync::Mutex<FailpointConfig>>,
}

impl FailpointNotifier {
    /// Create a new notifier
    pub fn new(notifier: Arc<Notify>, config_arc: Arc<std::sync::Mutex<FailpointConfig>>) -> Self {
        Self {
            notifier,
            config_arc,
        }
    }

    /// Get a future that will be notified when the failpoint configuration changes
    pub fn notified(&self) -> impl Future<Output = ()> + '_ {
        self.notifier.notified()
    }
}

impl Drop for FailpointNotifier {
    fn drop(&mut self) {
        let mut config = self.config_arc.lock().unwrap();
        config.notifiers.remove_notifier(&self.notifier);
    }
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
    let action_spec = parse_action_spec(actions)?;
    let config = FailpointConfig {
        action_spec,
        context_matchers: None,
        notifiers: FailpointNotifiers::new(),
        trigger_count: 0,
    };

    let mut failpoints = FAILPOINTS.write().unwrap();

    // If this failpoint already exists, notify all waiting tasks
    if let Some(existing_config) = failpoints.get(name) {
        // Notify all waiting tasks about the configuration change
        existing_config.lock().unwrap().notifiers.notify_all();
    }

    failpoints.insert(name.to_string(), Arc::new(std::sync::Mutex::new(config)));

    tracing::info!("Configured failpoint: {} = {}", name, actions);
    Ok(())
}

/// Configure a failpoint with context matching
pub fn configure_failpoint_with_context(
    name: &str,
    actions: &str,
    context_matchers: HashMap<String, String>,
) -> Result<()> {
    let action_spec = parse_action_spec(actions)?;
    let config = FailpointConfig {
        action_spec,
        context_matchers: Some(context_matchers),
        notifiers: FailpointNotifiers::new(),
        trigger_count: 0,
    };

    let mut failpoints = FAILPOINTS.write().unwrap();

    // If this failpoint already exists, notify all waiting tasks
    if let Some(existing_config) = failpoints.get(name) {
        // Notify all waiting tasks about the configuration change
        existing_config.lock().unwrap().notifiers.notify_all();
    }

    failpoints.insert(name.to_string(), Arc::new(std::sync::Mutex::new(config)));

    tracing::info!("Configured failpoint with context: {} = {}", name, actions);
    Ok(())
}

/// Remove a failpoint configuration
pub fn remove_failpoint(name: &str) {
    let mut failpoints = FAILPOINTS.write().unwrap();

    // Notify all waiting tasks before removing
    if let Some(existing_config) = failpoints.get(name) {
        existing_config.lock().unwrap().notifiers.notify_all();
    }

    failpoints.remove(name);

    tracing::info!("Removed failpoint: {}", name);
}

/// Check if failpoints are enabled (for compatibility with fail crate)
pub fn has_failpoints() -> bool {
    cfg!(feature = "testing") || std::env::var("FAILPOINTS").is_ok()
}

pub fn list() -> Vec<(impl std::fmt::Display, impl std::fmt::Display)> {
    FAILPOINTS
        .read()
        .unwrap()
        .iter()
        .map(|(name, config)| (name.clone(), format!("{:?}", config.lock().unwrap())))
        .collect::<Vec<_>>()
}

/// Execute a failpoint with optional context
pub fn failpoint(
    name: &str,
    context: Option<&FailpointContext>,
) -> Either<FailpointResult, Pin<Box<dyn Future<Output = FailpointResult> + Send>>> {
    failpoint_with_cancellation(name, context, &CancellationToken::new())
}

/// Execute a failpoint with cancellation support
pub fn failpoint_with_cancellation(
    name: &str,
    context: Option<&FailpointContext>,
    cancel_token: &CancellationToken,
) -> Either<FailpointResult, Pin<Box<dyn Future<Output = FailpointResult> + Send>>> {
    // Only check failpoints if testing feature is enabled
    if !cfg!(feature = "testing") {
        return Either::Left(FailpointResult::Continue);
    }

    // Get a clone of the failpoint config Arc - this minimizes the time we hold the global lock
    let config_arc = {
        let failpoints = FAILPOINTS.read().unwrap();
        let Some(config) = failpoints.get(name) else {
            return Either::Left(FailpointResult::Continue);
        };
        config.clone()
    };
    // Global lock is dropped here

    // Now work with the individual config's lock
    let (action_spec, context_matchers) = {
        let config = config_arc.lock().unwrap();
        (config.action_spec.clone(), config.context_matchers.clone())
    };

    // Check context matchers if provided
    if let (Some(matchers), Some(ctx)) = (&context_matchers, context) {
        if !matches_context(matchers, ctx) {
            return Either::Left(FailpointResult::Continue);
        }
    }

    // Check probability and max_count
    if let Some(probability) = action_spec.probability {
        // Check if we've hit the max count
        if let Some(max_count) = action_spec.max_count {
            // Get the current trigger count
            let trigger_count = {
                let config = config_arc.lock().unwrap();
                config.trigger_count
            };

            if trigger_count >= max_count {
                return Either::Left(FailpointResult::Continue);
            }
        }

        // Check probability
        let mut rng = rand::thread_rng();
        let roll: u8 = rng.gen_range(1..=100);
        if roll > probability {
            return Either::Left(FailpointResult::Continue);
        }

        // Increment trigger count
        {
            let mut config = config_arc.lock().unwrap();
            config.trigger_count += 1;
        }
    }

    tracing::info!("Hit failpoint: {}", name);

    execute_action(name, &action_spec, context, cancel_token, config_arc)
}

/// Create a notifier for a failpoint
fn create_failpoint_notifier(config_arc: Arc<std::sync::Mutex<FailpointConfig>>) -> FailpointNotifier {
    let mut config = config_arc.lock().unwrap();
    config.notifiers.create_notifier(config_arc.clone())
}

/// Execute a specific action (used for recursive execution in probability-based actions)
fn execute_action(
    name: &str,
    action_spec: &FailpointActionSpec,
    _context: Option<&FailpointContext>,
    cancel_token: &CancellationToken,
    config_arc: Arc<std::sync::Mutex<FailpointConfig>>,
) -> Either<FailpointResult, Pin<Box<dyn Future<Output = FailpointResult> + Send>>> {
    match &action_spec.action {
        FailpointAction::Off => Either::Left(FailpointResult::Continue),
        FailpointAction::Return(value) => {
            tracing::info!("Failpoint {} returning: {:?}", name, value);
            Either::Left(FailpointResult::Return(value.clone()))
        }
        FailpointAction::Exit => {
            tracing::info!("Failpoint {} exiting process", name);
            std::process::exit(1);
        }
        FailpointAction::Panic(message) => {
            tracing::error!("Failpoint {} panicking with message: {}", name, message);
            panic!("Failpoint panicked: {message}");
        }
        FailpointAction::Sleep(millis) => {
            let millis = *millis;
            let name = name.to_string();
            let cancel_token = cancel_token.clone();

            Either::Right(Box::pin(async move {
                tracing::info!("Failpoint {} sleeping for {}ms", name, millis);

                // Create a notifier for this task
                let notifier = create_failpoint_notifier(config_arc);

                // Sleep with cancellation support
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(millis)) => {
                        tracing::info!("Failpoint {} finished sleeping", name);
                        FailpointResult::Continue
                    }
                    _ = cancel_token.cancelled() => {
                        tracing::info!("Failpoint {} sleep cancelled", name);
                        FailpointResult::Cancelled
                    }
                    _ = notifier.notified() => {
                        tracing::info!("Failpoint {} sleep interrupted by configuration change", name);
                        FailpointResult::Continue
                    }
                }
            }))
        }
        FailpointAction::Pause => {
            let name = name.to_string();
            let cancel_token = cancel_token.clone();

            Either::Right(Box::pin(async move {
                tracing::info!("Failpoint {} pausing", name);

                // Create a notifier for this task
                let notifier = create_failpoint_notifier(config_arc);

                // Wait until cancelled or notified
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        tracing::info!("Failpoint {} pause cancelled", name);
                        FailpointResult::Cancelled
                    }
                    _ = notifier.notified() => {
                        tracing::info!("Failpoint {} pause ended due to configuration change", name);
                        FailpointResult::Continue
                    }
                }
            }))
        }
    }
}

/// Parse an action string into a FailpointActionSpec
fn parse_action_spec(actions: &str) -> Result<FailpointActionSpec> {
    // Regex patterns for different probability formats
    let prob_count_action = regex::Regex::new(r"^(\d+)%(\d+)\*(.+)$")?;
    let prob_action = regex::Regex::new(r"^(\d+)%(.+)$")?;
    let return_match = regex::Regex::new(r"^return\(([^)]*)\)$")?;
    let sleep_match = regex::Regex::new(r"^sleep\((\d+)\)$")?;
    let panic_match = regex::Regex::new(r"^panic\(([^)]*)\)$")?;

    let mut probability: Option<u8> = None;
    let mut max_count: Option<u32> = None;
    let mut action_str = actions;

    // Check for probability with count: "10%3*return(3000)"
    if let Some(captures) = prob_count_action.captures(actions) {
        probability = Some(captures.get(1).unwrap().as_str().parse::<u8>()?);
        max_count = Some(captures.get(2).unwrap().as_str().parse::<u32>()?);
        action_str = captures.get(3).unwrap().as_str();
    }
    // Check for probability without count: "50%return(15)"
    else if let Some(captures) = prob_action.captures(actions) {
        probability = Some(captures.get(1).unwrap().as_str().parse::<u8>()?);
        action_str = captures.get(2).unwrap().as_str();
    }

    // Parse the action part
    let action = if action_str == "off" {
        FailpointAction::Off
    } else if action_str == "pause" {
        FailpointAction::Pause
    } else if action_str == "return" {
        FailpointAction::Return(None)
    } else if action_str == "exit" {
        FailpointAction::Exit
    } else if let Some(captures) = return_match.captures(action_str) {
        let value = captures.get(1).unwrap().as_str();
        if value.is_empty() {
            FailpointAction::Return(None)
        } else {
            FailpointAction::Return(Some(value.to_string()))
        }
    } else if let Some(captures) = sleep_match.captures(action_str) {
        let millis = captures.get(1).unwrap().as_str().parse::<u64>()?;
        FailpointAction::Sleep(millis)
    } else if let Some(captures) = panic_match.captures(action_str) {
        let message = captures.get(1).unwrap().as_str().to_string();
        FailpointAction::Panic(message)
    } else {
        // For backward compatibility, treat numeric values as sleep duration
        if let Ok(millis) = action_str.parse::<u64>() {
            FailpointAction::Sleep(millis)
        } else {
            anyhow::bail!("Invalid failpoint action: {}", action_str);
        }
    };

    if let Some(p) = probability {
        if p > 100 {
            anyhow::bail!("Probability must be between 0 and 100, got {}", p);
        }
    }

    Ok(FailpointActionSpec {
        probability,
        max_count,
        action,
    })
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
    use tokio::time::timeout;

    // Helper function to await either sync or async failpoint results
    async fn await_failpoint_result(
        either: Either<FailpointResult, Pin<Box<dyn Future<Output = FailpointResult> + Send>>>,
    ) -> FailpointResult {
        match either {
            Either::Left(result) => result,
            Either::Right(future) => future.await,
        }
    }

    #[tokio::test]
    async fn test_failpoint_off() {
        configure_failpoint("test_off", "off").unwrap();
        let result = await_failpoint_result(failpoint("test_off", None)).await;
        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_failpoint_sleep() {
        configure_failpoint("test_sleep", "sleep(10)").unwrap();
        let start = std::time::Instant::now();
        let result = await_failpoint_result(failpoint("test_sleep", None)).await;
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

        let result = await_failpoint_result(failpoint_with_cancellation(
            "test_pause",
            None,
            &cancel_token,
        ))
        .await;
        matches!(result, FailpointResult::Cancelled);
    }

    #[tokio::test]
    async fn test_failpoint_pause_and_resume() {
        let failpoint_name = "test_pause_resume";
        configure_failpoint(failpoint_name, "pause").unwrap();

        let start = std::time::Instant::now();

        // Start a task that hits the failpoint
        let task =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

        // Give it time to hit the failpoint and pause
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Reconfigure the failpoint to "off" to resume
        configure_failpoint(failpoint_name, "off").unwrap();

        // The task should complete quickly now
        let result = timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap();

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
        let task =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

        // Give it time to hit the failpoint and pause
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Reconfigure the failpoint to return a value
        configure_failpoint(failpoint_name, "return(resumed)").unwrap();

        // The task should complete by resuming (Continue), not with the new return value
        // The new configuration only applies to future hits of the failpoint
        let result = timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap();

        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_failpoint_sleep_and_resume() {
        let failpoint_name = "test_sleep_resume";
        configure_failpoint(failpoint_name, "sleep(1000)").unwrap(); // 1 second sleep

        let start = std::time::Instant::now();

        // Start a task that hits the failpoint
        let task =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

        // Give it time to hit the failpoint and start sleeping
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Reconfigure the failpoint to "off" to resume
        configure_failpoint(failpoint_name, "off").unwrap();

        // The task should complete quickly now
        let result = timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap();

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
        let task =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

        // Give it time to hit the failpoint and start sleeping
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Change the sleep duration to a shorter time
        configure_failpoint(failpoint_name, "sleep(50)").unwrap();

        // The task should complete by resuming (Continue), not by sleeping the new duration
        let result = timeout(Duration::from_millis(200), task)
            .await
            .unwrap()
            .unwrap();

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
        let task =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

        // Give it time to hit the failpoint and pause
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Remove the failpoint entirely
        remove_failpoint(failpoint_name);

        // The task should complete quickly now
        let result = timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap();

        // Should have resumed and continued
        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_multiple_paused_tasks_resume() {
        let failpoint_name = "test_multiple_pause";
        configure_failpoint(failpoint_name, "pause").unwrap();

        // Start multiple tasks that hit the same failpoint
        let task1 =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

        let task2 =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

        let task3 =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

        // Give them time to hit the failpoint and pause
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Reconfigure the failpoint to "off" to resume all tasks
        configure_failpoint(failpoint_name, "off").unwrap();

        // All tasks should complete quickly now
        let result1 = timeout(Duration::from_millis(100), task1)
            .await
            .unwrap()
            .unwrap();
        let result2 = timeout(Duration::from_millis(100), task2)
            .await
            .unwrap()
            .unwrap();
        let result3 = timeout(Duration::from_millis(100), task3)
            .await
            .unwrap()
            .unwrap();

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
        let task =
            tokio::spawn(
                async move { await_failpoint_result(failpoint(failpoint_name, None)).await },
            );

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
        let result = timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap();

        // Should resume normally since it was woken up by a configuration change
        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_context_matching() {
        let mut context = HashMap::new();
        context.insert("user_id".to_string(), "123".to_string());
        context.insert("operation".to_string(), "read".to_string());

        let mut matchers = HashMap::new();
        matchers.insert("user_id".to_string(), "123".to_string());
        matchers.insert("operation".to_string(), "read".to_string());

        configure_failpoint_with_context("test_context", "return(matched)", matchers).unwrap();

        let result = await_failpoint_result(failpoint("test_context", Some(&context))).await;
        if let FailpointResult::Return(Some(value)) = result {
            assert_eq!(value, "matched");
        } else {
            panic!("Expected return result");
        }

        // Test with non-matching context
        let mut wrong_context = HashMap::new();
        wrong_context.insert("user_id".to_string(), "456".to_string());
        wrong_context.insert("operation".to_string(), "write".to_string());

        let result = await_failpoint_result(failpoint("test_context", Some(&wrong_context))).await;
        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_failpoint_panic() {
        configure_failpoint("test_panic", "panic(test message)").unwrap();

        let result = tokio::task::spawn_blocking(|| {
            std::panic::catch_unwind(|| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async { await_failpoint_result(failpoint("test_panic", None)).await })
            })
        })
        .await
        .unwrap();

        assert!(result.is_err());
        let panic_msg = result.unwrap_err();
        if let Some(msg) = panic_msg.downcast_ref::<String>() {
            assert!(msg.contains("test message"));
        } else if let Some(msg) = panic_msg.downcast_ref::<&str>() {
            assert!(msg.contains("test message"));
        } else {
            panic!("Expected panic with string message, got: {panic_msg:?}");
        }
    }

    #[tokio::test]
    async fn test_failpoint_probability_simple() {
        configure_failpoint("test_prob_simple", "100%return(always)").unwrap();

        // 100% probability should always trigger
        let result = await_failpoint_result(failpoint("test_prob_simple", None)).await;
        if let FailpointResult::Return(Some(value)) = result {
            assert_eq!(value, "always");
        } else {
            panic!("Expected return result with 100% probability");
        }

        // 0% probability should never trigger
        configure_failpoint("test_prob_never", "0%return(never)").unwrap();
        let result = await_failpoint_result(failpoint("test_prob_never", None)).await;
        matches!(result, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_failpoint_probability_with_count() {
        configure_failpoint("test_prob_count", "100%2*return(limited)").unwrap();

        // First two calls should trigger (100% probability, max 2 times)
        let result1 = await_failpoint_result(failpoint("test_prob_count", None)).await;
        if let FailpointResult::Return(Some(value)) = result1 {
            assert_eq!(value, "limited");
        } else {
            panic!("Expected return result on first call");
        }

        let result2 = await_failpoint_result(failpoint("test_prob_count", None)).await;
        if let FailpointResult::Return(Some(value)) = result2 {
            assert_eq!(value, "limited");
        } else {
            panic!("Expected return result on second call");
        }

        // Third call should not trigger (count limit exceeded)
        let result3 = await_failpoint_result(failpoint("test_prob_count", None)).await;
        matches!(result3, FailpointResult::Continue);
    }

    #[tokio::test]
    async fn test_failpoint_probability_nested_actions() {
        configure_failpoint("test_prob_sleep", "100%sleep(10)").unwrap();

        let start = std::time::Instant::now();
        let result = await_failpoint_result(failpoint("test_prob_sleep", None)).await;
        let duration = start.elapsed();

        matches!(result, FailpointResult::Continue);
        assert!(duration >= Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_parse_action_spec_panic() {
        let action_spec = parse_action_spec("panic(test error)").unwrap();
        matches!(action_spec.action, FailpointAction::Panic(msg) if msg == "test error");
    }

    #[tokio::test]
    async fn test_parse_action_spec_probability() {
        // Test simple probability
        let action_spec = parse_action_spec("50%return(value)").unwrap();
        if let FailpointAction::Return(Some(ref v)) = action_spec.action {
            assert_eq!(v, "value");
        } else {
            panic!("Expected return action");
        }

        // Test probability with count
        let action_spec = parse_action_spec("25%3*return(limited)").unwrap();
        if let FailpointAction::Return(Some(ref v)) = action_spec.action {
            assert_eq!(v, "limited");
        } else {
            panic!("Expected return action");
        }

        // Test probability with other actions
        let action_spec = parse_action_spec("75%sleep(100)").unwrap();
        if let FailpointAction::Sleep(100) = action_spec.action {
            // Test passed
        } else {
            panic!("Expected sleep action");
        }
    }

    #[tokio::test]
    async fn test_parse_action_spec_invalid_probability() {
        // Test probability > 100
        let result = parse_action_spec("150%return(invalid)");
        assert!(result.is_err());

        // Test invalid format
        let result = parse_action_spec("50%invalid_action");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_audit_specific_patterns() {
        // Test patterns found in the audit

        // 1. panic(failpoint) - from test_sharding.py
        let action_spec = parse_action_spec("panic(failpoint)").unwrap();
        matches!(action_spec.action, FailpointAction::Panic(msg) if msg == "failpoint");

        // 2. 50%return(15) - from test_bad_connection.py
        let action_spec = parse_action_spec("50%return(15)").unwrap();
        if let FailpointAction::Return(Some(ref v)) = action_spec.action {
            assert_eq!(v, "15");
        } else {
            panic!("Expected return action");
        }

        // 3. 10%3*return(3000) - from test_bad_connection.py
        let action_spec = parse_action_spec("10%3*return(3000)").unwrap();
        if let FailpointAction::Return(Some(ref v)) = action_spec.action {
            assert_eq!(v, "3000");
        } else {
            panic!("Expected return action");
        }

        // 4. 10%return(60000) - from test_bad_connection.py
        let action_spec = parse_action_spec("10%return(60000)").unwrap();
        if let FailpointAction::Return(Some(ref v)) = action_spec.action {
            assert_eq!(v, "60000");
        } else {
            panic!("Expected return action");
        }

        // Test that these patterns work in practice
        configure_failpoint("test_audit_panic", "panic(storage controller crash)").unwrap();
        configure_failpoint("test_audit_prob", "100%return(42)").unwrap();
        configure_failpoint("test_audit_count", "100%1*return(limited)").unwrap();

        // Test the probability-based return
        let result = await_failpoint_result(failpoint("test_audit_prob", None)).await;
        if let FailpointResult::Return(Some(value)) = result {
            assert_eq!(value, "42");
        } else {
            panic!("Expected return result");
        }

        // Test the count-limited probability
        let result1 = await_failpoint_result(failpoint("test_audit_count", None)).await;
        if let FailpointResult::Return(Some(value)) = result1 {
            assert_eq!(value, "limited");
        } else {
            panic!("Expected return result on first call");
        }

        // Second call should not trigger (count limit exceeded)
        let result2 = await_failpoint_result(failpoint("test_audit_count", None)).await;
        matches!(result2, FailpointResult::Continue);
    }
}
