// rustc lints/lint groups
// https://doc.rust-lang.org/rustc/lints/groups.html
#![deny(deprecated, future_incompatible, let_underscore, nonstandard_style)]
#![warn(clippy::all, clippy::pedantic, clippy::cargo)]
// List of denied lints from the clippy::restriction group.
// https://rust-lang.github.io/rust-clippy/master/index.html#?groups=restriction
#![warn(
    clippy::undocumented_unsafe_blocks,
    // TODO: Enable once all individual checks are enabled.
    //clippy::as_conversions,
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
    clippy::unimplemented,
    clippy::unwrap_used,
)]
// List of permanently allowed lints.
#![allow(
    // It's ok to cast bool to u8, etc.
    clippy::cast_lossless,
    // Seems unavoidable.
    clippy::multiple_crate_versions,
    // While #[must_use] is a great feature this check is too noisy.
    clippy::must_use_candidate,
    // Inline consts, structs, fns, imports, etc. are ok if they're used by
    // the following statement(s).
    clippy::items_after_statements,
)]
// List of temporarily allowed lints.
// TODO: fix code and reduce list or move to permanent list above.
#![expect(
    clippy::cargo_common_metadata,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::inline_always,
    clippy::match_same_arms,
    clippy::match_wild_err_arm,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::similar_names,
    clippy::single_match_else,
    clippy::struct_excessive_bools,
    clippy::struct_field_names,
    clippy::too_many_lines,
    clippy::unused_self
)]
#![cfg_attr(
    any(test, feature = "testing"),
    allow(
        clippy::needless_raw_string_hashes,
        clippy::unreadable_literal,
        clippy::unused_async,
    )
)]
// List of temporarily allowed lints to unblock beta/nightly.
#![allow(unknown_lints)]

pub mod binary;

mod auth;
mod cache;
mod cancellation;
mod compute;
mod compute_ctl;
mod config;
mod console_redirect_proxy;
mod context;
mod control_plane;
mod error;
mod ext;
mod http;
mod intern;
mod jemalloc;
mod logging;
mod metrics;
mod parse;
mod protocol2;
mod proxy;
mod rate_limiter;
mod redis;
mod sasl;
mod scram;
mod serverless;
mod signals;
mod stream;
mod tls;
mod types;
mod url;
mod usage_metrics;
mod waiters;
