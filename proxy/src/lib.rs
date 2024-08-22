// rustc lints/lint groups
// https://doc.rust-lang.org/rustc/lints/groups.html
#![deny(
    deprecated,
    future_incompatible,
    // TODO: consider let_underscore
    nonstandard_style,
    rust_2024_compatibility
)]
#![warn(clippy::all, clippy::pedantic, clippy::cargo)]
// List of denied lints from the clippy::restriction group.
// https://rust-lang.github.io/rust-clippy/master/index.html#?groups=restriction
#![warn(
    clippy::undocumented_unsafe_blocks,
    clippy::dbg_macro,
    clippy::empty_enum_variants_with_brackets,
    clippy::exit,
    clippy::float_cmp_const,
    clippy::lossy_float_literal,
    clippy::macro_use_imports,
    clippy::manual_ok_or,
    // TODO: consider clippy::map_err_ignore
    // TODO: consider clippy::mem_forget
    clippy::rc_mutex,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::string_add,
    clippy::string_to_string,
    clippy::todo,
    // TODO: consider clippy::unimplemented
    // TODO: consider clippy::unwrap_used
)]
// List of permanently allowed lints.
#![allow(
    // It's ok to cast u8 to bool, etc.
    clippy::cast_lossless,
)]
// List of temporarily allowed lints.
// TODO: Switch to except() once stable with 1.81.
// TODO: fix code and reduce list or move to permanent list above.
#![allow(
    clippy::cargo_common_metadata,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::explicit_iter_loop,
    clippy::float_cmp,
    clippy::if_not_else,
    clippy::ignored_unit_patterns,
    clippy::implicit_hasher,
    clippy::inconsistent_struct_constructor,
    clippy::inline_always,
    clippy::items_after_statements,
    clippy::manual_assert,
    clippy::manual_let_else,
    clippy::manual_string_new,
    clippy::match_bool,
    clippy::match_same_arms,
    clippy::match_wild_err_arm,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::multiple_crate_versions,
    clippy::must_use_candidate,
    clippy::needless_for_each,
    clippy::needless_pass_by_value,
    clippy::needless_raw_string_hashes,
    clippy::option_as_ref_cloned,
    clippy::redundant_closure_for_method_calls,
    clippy::redundant_else,
    clippy::return_self_not_must_use,
    clippy::similar_names,
    clippy::single_char_pattern,
    clippy::single_match_else,
    clippy::struct_excessive_bools,
    clippy::struct_field_names,
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::unnested_or_patterns,
    clippy::unreadable_literal,
    clippy::unused_async,
    clippy::unused_self,
    clippy::used_underscore_binding,
    clippy::wildcard_imports
)]
// List of temporarily allowed lints to unblock beta/nightly.
#![allow(unknown_lints, clippy::manual_inspect)]

use std::convert::Infallible;

use anyhow::{bail, Context};
use intern::{EndpointIdInt, EndpointIdTag, InternId};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub mod auth;
pub mod cache;
pub mod cancellation;
pub mod compute;
pub mod config;
pub mod console;
pub mod context;
pub mod error;
pub mod http;
pub mod intern;
pub mod jemalloc;
pub mod logging;
pub mod metrics;
pub mod parse;
pub mod protocol2;
pub mod proxy;
pub mod rate_limiter;
pub mod redis;
pub mod sasl;
pub mod scram;
pub mod serverless;
pub mod stream;
pub mod url;
pub mod usage_metrics;
pub mod waiters;

/// Handle unix signals appropriately.
pub async fn handle_signals(token: CancellationToken) -> anyhow::Result<Infallible> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut hangup = signal(SignalKind::hangup())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;

    loop {
        tokio::select! {
            // Hangup is commonly used for config reload.
            _ = hangup.recv() => {
                warn!("received SIGHUP; config reload is not supported");
            }
            // Shut down the whole application.
            _ = interrupt.recv() => {
                warn!("received SIGINT, exiting immediately");
                bail!("interrupted");
            }
            _ = terminate.recv() => {
                warn!("received SIGTERM, shutting down once all existing connections have closed");
                token.cancel();
            }
        }
    }
}

/// Flattens `Result<Result<T>>` into `Result<T>`.
pub fn flatten_err<T>(r: Result<anyhow::Result<T>, JoinError>) -> anyhow::Result<T> {
    r.context("join error").and_then(|x| x)
}

macro_rules! smol_str_wrapper {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(smol_str::SmolStr);

        impl $name {
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl<T> std::cmp::PartialEq<T> for $name
        where
            smol_str::SmolStr: std::cmp::PartialEq<T>,
        {
            fn eq(&self, other: &T) -> bool {
                self.0.eq(other)
            }
        }

        impl<T> From<T> for $name
        where
            smol_str::SmolStr: From<T>,
        {
            fn from(x: T) -> Self {
                Self(x.into())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str {
                &*self.0
            }
        }

        impl<'de> serde::de::Deserialize<'de> for $name {
            fn deserialize<D: serde::de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                <smol_str::SmolStr as serde::de::Deserialize<'de>>::deserialize(d).map(Self)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(s)
            }
        }
    };
}

const POOLER_SUFFIX: &str = "-pooler";

impl EndpointId {
    fn normalize(&self) -> Self {
        if let Some(stripped) = self.as_ref().strip_suffix(POOLER_SUFFIX) {
            stripped.into()
        } else {
            self.clone()
        }
    }

    fn normalize_intern(&self) -> EndpointIdInt {
        if let Some(stripped) = self.as_ref().strip_suffix(POOLER_SUFFIX) {
            EndpointIdTag::get_interner().get_or_intern(stripped)
        } else {
            self.into()
        }
    }
}

// 90% of role name strings are 20 characters or less.
smol_str_wrapper!(RoleName);
// 50% of endpoint strings are 23 characters or less.
smol_str_wrapper!(EndpointId);
// 50% of branch strings are 23 characters or less.
smol_str_wrapper!(BranchId);
// 90% of project strings are 23 characters or less.
smol_str_wrapper!(ProjectId);

// will usually equal endpoint ID
smol_str_wrapper!(EndpointCacheKey);

smol_str_wrapper!(DbName);

// postgres hostname, will likely be a port:ip addr
smol_str_wrapper!(Host);

// Endpoints are a bit tricky. Rare they might be branches or projects.
impl EndpointId {
    pub fn is_endpoint(&self) -> bool {
        self.0.starts_with("ep-")
    }
    pub fn is_branch(&self) -> bool {
        self.0.starts_with("br-")
    }
    pub fn is_project(&self) -> bool {
        !self.is_endpoint() && !self.is_branch()
    }
    pub fn as_branch(&self) -> BranchId {
        BranchId(self.0.clone())
    }
    pub fn as_project(&self) -> ProjectId {
        ProjectId(self.0.clone())
    }
}
