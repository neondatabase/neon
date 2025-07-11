//! Macros for convenient failpoint usage

/// Simple failpoint macro - async version that doesn't require spawn_blocking
#[macro_export]
macro_rules! fail_point {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None).await {
                $crate::FailpointResult::Continue => {},
                $crate::FailpointResult::Return(value) => {
                    // For compatibility with fail crate, we need to handle the return value
                    // This will be used in closures like |x| x pattern
                    return Ok(value.parse().unwrap_or_default());
                },
                $crate::FailpointResult::Cancelled => {},
            }
        }
    }};
    ($name:literal, |$var:ident| $body:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None).await {
                $crate::FailpointResult::Continue => {},
                $crate::FailpointResult::Return(value) => {
                    let $var = value.as_str();
                    $body
                },
                $crate::FailpointResult::Cancelled => {},
            }
        }
    }};
}

/// Failpoint macro with context support
#[macro_export]
macro_rules! fail_point_with_context {
    ($name:literal, $context:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)).await {
                $crate::FailpointResult::Continue => {},
                $crate::FailpointResult::Return(value) => {
                    return Ok(value.parse().unwrap_or_default());
                },
                $crate::FailpointResult::Cancelled => {},
            }
        }
    }};
    ($name:literal, $context:expr, |$var:ident| $body:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)).await {
                $crate::FailpointResult::Continue => {},
                $crate::FailpointResult::Return(value) => {
                    let $var = value.as_str();
                    $body
                },
                $crate::FailpointResult::Cancelled => {},
            }
        }
    }};
}

/// Pausable failpoint macro - equivalent to the old pausable_failpoint
#[macro_export]
macro_rules! pausable_failpoint {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            let cancel = ::tokio_util::sync::CancellationToken::new();
            let _ = $crate::pausable_failpoint!($name, &cancel);
        }
    }};
    ($name:literal, $cancel:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint_with_cancellation($name, None, $cancel).await {
                $crate::FailpointResult::Continue => Ok(()),
                $crate::FailpointResult::Return(_) => Ok(()),
                $crate::FailpointResult::Cancelled => Err(()),
            }
        } else {
            Ok(())
        }
    }};
}

/// Sleep failpoint macro - for async sleep operations
#[macro_export]
macro_rules! sleep_millis_async {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            $crate::failpoint($name, None).await;
        }
    }};
    ($name:literal, $cancel:expr) => {{
        if cfg!(feature = "testing") {
            $crate::failpoint_with_cancellation($name, None, $cancel).await;
        }
    }};
}

/// Convenience macro for creating failpoint context
#[macro_export]
macro_rules! failpoint_context {
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut context = ::std::collections::HashMap::new();
        $(
            context.insert($key.to_string(), $value.to_string());
        )*
        context
    }};
}

/// Macro for simple failpoint calls that might return early
#[macro_export]
macro_rules! failpoint_return {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            if let $crate::FailpointResult::Return(value) = $crate::failpoint($name, None).await {
                return value.parse().unwrap_or_default();
            }
        }
    }};
    ($name:literal, $context:expr) => {{
        if cfg!(feature = "testing") {
            if let $crate::FailpointResult::Return(value) = $crate::failpoint($name, Some($context)).await {
                return value.parse().unwrap_or_default();
            }
        }
    }};
}

/// Macro for failpoint calls that might bail with an error
#[macro_export]
macro_rules! failpoint_bail {
    ($name:literal, $error_msg:literal) => {{
        if cfg!(feature = "testing") {
            if let $crate::FailpointResult::Return(_) = $crate::failpoint($name, None).await {
                anyhow::bail!($error_msg);
            }
        }
    }};
    ($name:literal, $context:expr, $error_msg:literal) => {{
        if cfg!(feature = "testing") {
            if let $crate::FailpointResult::Return(_) = $crate::failpoint($name, Some($context)).await {
                anyhow::bail!($error_msg);
            }
        }
    }};
}

// Re-export for convenience
pub use fail_point;
pub use fail_point_with_context;
pub use failpoint_bail;
pub use failpoint_context;
pub use failpoint_return;
pub use pausable_failpoint;
pub use sleep_millis_async; 