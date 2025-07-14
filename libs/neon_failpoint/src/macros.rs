//! Macros for convenient failpoint usage

/// Simple failpoint macro - async version that doesn't require spawn_blocking
#[macro_export]
macro_rules! fail_point {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                $crate::either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                $crate::either::Either::Right(future) => {
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
                $crate::either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                $crate::either::Either::Right(future) => {
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
                    $crate::either::Either::Left(result) => {
                        match result {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                    $crate::either::Either::Right(future) => {
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
                $crate::either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                $crate::either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_sync! was used. Use fail_point! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $closure:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, None) {
                $crate::either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                $crate::either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_sync! was used. Use fail_point! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $condition:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            if $condition {
                match $crate::failpoint($name, None) {
                    $crate::either::Either::Left(result) => {
                        match result {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                    $crate::either::Either::Right(_) => {
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
                $crate::either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                $crate::either::Either::Right(future) => {
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
                $crate::either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                $crate::either::Either::Right(future) => {
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
                    $crate::either::Either::Left(result) => {
                        match result {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                    $crate::either::Either::Right(future) => {
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
                $crate::either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(_) => {
                            panic!("failpoint was configured with return(X) but Rust code does not pass a closure to map X to a return value");
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                $crate::either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_with_context_sync! was used. Use fail_point_with_context! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $context:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint($name, Some($context)) {
                $crate::either::Either::Left(result) => {
                    match result {
                        $crate::FailpointResult::Continue => {},
                        $crate::FailpointResult::Return(value) => {
                            let closure = $closure;
                            return closure(value);
                        },
                        $crate::FailpointResult::Cancelled => {},
                    }
                },
                $crate::either::Either::Right(_) => {
                    panic!("failpoint '{}' triggered an async action (sleep/pause) but fail_point_with_context_sync! was used. Use fail_point_with_context! instead.", $name);
                },
            }
        }
    }};
    ($name:literal, $context:expr, $condition:expr, $closure:expr) => {{
        if cfg!(feature = "testing") {
            if $condition {
                match $crate::failpoint($name, Some($context)) {
                    $crate::either::Either::Left(result) => {
                        match result {
                            $crate::FailpointResult::Continue => {},
                            $crate::FailpointResult::Return(value) => {
                                let closure = $closure;
                                return closure(value);
                            },
                            $crate::FailpointResult::Cancelled => {},
                        }
                    },
                    $crate::either::Either::Right(_) => {
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
            ::tracing::info!("at failpoint {}", $name); // tests rely on this
            match $crate::failpoint_with_cancellation($name, None, $cancel) {
                $crate::either::Either::Left(result) => match result {
                    $crate::FailpointResult::Continue => Ok(()),
                    $crate::FailpointResult::Return(_) => Ok(()),
                    $crate::FailpointResult::Cancelled => Err(()),
                },
                $crate::either::Either::Right(future) => match future.await {
                    $crate::FailpointResult::Continue => Ok(()),
                    $crate::FailpointResult::Return(_) => Ok(()),
                    $crate::FailpointResult::Cancelled => Err(()),
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
                $crate::either::Either::Left(_) => {}
                $crate::either::Either::Right(future) => {
                    future.await;
                }
            }
        }
    }};
    ($name:literal, $cancel:expr) => {{
        if cfg!(feature = "testing") {
            match $crate::failpoint_with_cancellation($name, None, $cancel) {
                $crate::either::Either::Left(_) => {}
                $crate::either::Either::Right(future) => {
                    future.await;
                }
            }
        }
    }};
}

// Re-export for convenience
pub use fail_point;
pub use fail_point_sync;
pub use fail_point_with_context;
pub use fail_point_with_context_sync;
pub use pausable_failpoint;
pub use sleep_millis_async;
