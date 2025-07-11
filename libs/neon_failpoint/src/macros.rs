//! Macros for convenient failpoint usage

/// Simple failpoint macro - async version that doesn't require spawn_blocking
#[macro_export]
macro_rules! fail_point {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                either::Either::Right(future) => {
                    match future.await {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
            }
        }
    }};
    ($name:literal, $closure:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                either::Either::Right(future) => {
                    match future.await {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
            }
        }
    }};
    ($name:literal, $condition:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            if $condition {
                match $crate::failpoint($name, None) {
                    either::Either::Left(result) => {
                        match result {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                    either::Either::Right(future) => {
                        match future.await {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                }
            }
        }
    }};
}

/// Simple failpoint macro - sync version that panics if async action is triggered
#[macro_export]
macro_rules! fail_point_sync {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_sync! was used. Use fail_point! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $closure:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_sync! was used. Use fail_point! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $condition:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            if $condition {
                match $crate::failpoint($name, None) {
                    either::Either::Left(result) => {
                        match result {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                    either::Either::Right(_) => {
                        panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_sync! was used. Use fail_point! instead.", $name);
                    },
                }
            }
        }
    }};
}

/// Failpoint macro with context support
#[macro_export]
macro_rules! fail_point_with_context {
    ($name:literal, $context:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                either::Either::Right(future) => {
                    match future.await {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
            }
        }
    }};
    ($name:literal, $context:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                either::Either::Right(future) => {
                    match future.await {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
            }
        }
    }};
    ($name:literal, $context:expr, $condition:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            if $condition {
                match $crate::failpoint($name, Some($context)) {
                    either::Either::Left(result) => {
                        match result {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                    either::Either::Right(future) => {
                        match future.await {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                }
            }
        }
    }};
}

/// Failpoint macro with context support - sync version
#[macro_export]
macro_rules! fail_point_with_context_sync {
    ($name:literal, $context:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_with_context_sync! was used. Use fail_point_with_context! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $context:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_with_context_sync! was used. Use fail_point_with_context! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $context:expr, $condition:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            if $condition {
                match $crate::failpoint($name, Some($context)) {
                    either::Either::Left(result) => {
                        match result {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                    either::Either::Right(_) => {
                        panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_with_context_sync! was used. Use fail_point_with_context! instead.", $name);
                    },
                }
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
            match $crate::failpoint_with_cancellation($name, None, $cancel) {
                either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => Ok(()),
                        $crate::FailpointResult::Return(_) => Ok(()),
                        $crate::FailpointResult::Cancelled => Err(()),
                    }
                },
                either::Either::Right(future) => {
                    match future.await {
                        $crate::FailpointResult::Continue => Ok(()),
                        $crate::FailpointResult::Return(_) => Ok(()),
                        $crate::FailpointResult::Cancelled => Err(()),
                    }
                },
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
            match $crate::failpoint($name, None) {
                either::Either::Left(_) => {},
                either::Either::Right(future) => {
                    future.await;
                },
            }
        }
    }};
    ($name:literal, $cancel:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint_with_cancellation($name, None, $cancel) {
                either::Either::Left(_) => {},
                either::Either::Right(future) => {
                    future.await;
                },
            }
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
            match $crate::failpoint($name, None) {
                either::Either::Left(result) => {
                    if let $crate::FailpointResult::Return(Some(value)) = result {
                        return value.parse().unwrap_or_default();
                    }
                },
                either::Either::Right(future) => {
                    if let $crate::FailpointResult::Return(Some(value)) = future.await {
                        return value.parse().unwrap_or_default();
                    }
                },
            }
        }
    }};
    ($name:literal, $context:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                either::Either::Left(result) => {
                    if let $crate::FailpointResult::Return(Some(value)) = result {
                        return value.parse().unwrap_or_default();
                    }
                },
                either::Either::Right(future) => {
                    if let $crate::FailpointResult::Return(Some(value)) = future.await {
                        return value.parse().unwrap_or_default();
                    }
                },
            }
        }
    }};
}

/// Macro for simple failpoint calls that might return early - sync version
#[macro_export]
macro_rules! failpoint_return_sync {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                either::Either::Left(result) => {
                    if let $crate::FailpointResult::Return(Some(value)) = result {
                        return value.parse().unwrap_or_default();
                    }
                },
                either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but failpoint_return_sync! was used. Use failpoint_return! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $context:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                either::Either::Left(result) => {
                    if let $crate::FailpointResult::Return(Some(value)) = result {
                        return value.parse().unwrap_or_default();
                    }
                },
                either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but failpoint_return_sync! was used. Use failpoint_return! instead.", $name);
                },
            }
        }
    }};
}

/// Macro for failpoint calls that might bail with an error
#[macro_export]
macro_rules! failpoint_bail {
    ($name:literal, $error_msg:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                either::Either::Left(result) => {
                    if let $crate::FailpointResult::Return(_) = result {
                        anyhow::bail!($error_msg);
                    }
                },
                either::Either::Right(future) => {
                    if let $crate::FailpointResult::Return(_) = future.await {
                        anyhow::bail!($error_msg);
                    }
                },
            }
        }
    }};
    ($name:literal, $context:expr, $error_msg:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                either::Either::Left(result) => {
                    if let $crate::FailpointResult::Return(_) = result {
                        anyhow::bail!($error_msg);
                    }
                },
                either::Either::Right(future) => {
                    if let $crate::FailpointResult::Return(_) = future.await {
                        anyhow::bail!($error_msg);
                    }
                },
            }
        }
    }};
}

/// Macro for failpoint calls that might bail with an error - sync version
#[macro_export]
macro_rules! failpoint_bail_sync {
    ($name:literal, $error_msg:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                either::Either::Left(result) => {
                    if let $crate::FailpointResult::Return(_) = result {
                        anyhow::bail!($error_msg);
                    }
                },
                either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but failpoint_bail_sync! was used. Use failpoint_bail! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $context:expr, $error_msg:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                either::Either::Left(result) => {
                    if let $crate::FailpointResult::Return(_) = result {
                        anyhow::bail!($error_msg);
                    }
                },
                either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but failpoint_bail_sync! was used. Use failpoint_bail! instead.", $name);
                },
            }
        }
    }};
}

// Re-export for convenience
pub use fail_point;
pub use fail_point_sync;
pub use fail_point_with_context;
pub use fail_point_with_context_sync;
pub use failpoint_bail;
pub use failpoint_bail_sync;
pub use failpoint_context;
pub use failpoint_return;
pub use failpoint_return_sync;
pub use pausable_failpoint;
pub use sleep_millis_async;
