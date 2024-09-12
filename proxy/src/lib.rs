// rustc lints/lint groups
// https://doc.rust-lang.org/rustc/lints/groups.html
#![deny(
    deprecated,
    future_incompatible,
    let_underscore,
    nonstandard_style,
    rust_2024_compatibility
)]
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
    // TODO: consider clippy::unimplemented
    // TODO: consider clippy::unwrap_used
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
#![allow(
    unknown_lints,
    // TODO: 1.82: Add `use<T>` where necessary and remove from this list.
    impl_trait_overcaptures,
)]

use std::{
    convert::Infallible,
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use anyhow::{bail, Context};
use bytes::{Buf, BufMut};
use config::TlsServerEndPoint;
use intern::{EndpointIdInt, EndpointIdTag, InternId};
use serde::{Deserialize, Serialize};
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
pub async fn handle_signals<F, Fut>(
    token: CancellationToken,
    mut refresh_config: F,
) -> anyhow::Result<Infallible>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    use tokio::signal::unix::{signal, SignalKind};

    let mut hangup = signal(SignalKind::hangup())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;

    loop {
        tokio::select! {
            // Hangup is commonly used for config reload.
            _ = hangup.recv() => {
                warn!("received SIGHUP");
                refresh_config().await?;
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
            #[allow(unused)]
            pub(crate) fn as_str(&self) -> &str {
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
    pub(crate) fn is_endpoint(&self) -> bool {
        self.0.starts_with("ep-")
    }
    pub(crate) fn is_branch(&self) -> bool {
        self.0.starts_with("br-")
    }
    // pub(crate) fn is_project(&self) -> bool {
    //     !self.is_endpoint() && !self.is_branch()
    // }
    pub(crate) fn as_branch(&self) -> BranchId {
        BranchId(self.0.clone())
    }
    pub(crate) fn as_project(&self) -> ProjectId {
        ProjectId(self.0.clone())
    }
}

pub struct PglbCodec;

impl tokio_util::codec::Encoder<PglbMessage> for PglbCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: PglbMessage, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        match item {
            PglbMessage::Control(ctrl) => {
                dst.put_u8(1);
                match ctrl {
                    PglbControlMessage::ConnectionInitiated(msg) => {
                        let encode = serde_json::to_string(&msg).context("ser")?;
                        dst.put_u32(1 + encode.len() as u32);
                        dst.put_u8(0);
                        dst.put(encode.as_bytes());
                    }
                    PglbControlMessage::ConnectToCompute { socket } => match socket {
                        SocketAddr::V4(v4) => {
                            dst.put_u32(1 + 4 + 2);
                            dst.put_u8(1);
                            dst.put_u32(v4.ip().to_bits());
                            dst.put_u16(v4.port());
                        }
                        SocketAddr::V6(v6) => {
                            dst.put_u32(1 + 16 + 2);
                            dst.put_u8(1);
                            dst.put_u128(v6.ip().to_bits());
                            dst.put_u16(v6.port());
                        }
                    },
                    PglbControlMessage::ComputeEstablish => {
                        dst.put_u32(1);
                        dst.put_u8(2);
                    }
                }
            }
            PglbMessage::Postgres(pg) => {
                dst.put_u8(0);
                dst.put_u32(pg.len() as u32);
                dst.put(pg);
            }
        }
        Ok(())
    }
}

impl tokio_util::codec::Decoder for PglbCodec {
    type Item = PglbMessage;
    type Error = anyhow::Error;

    fn decode(&mut self, dst: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if dst.remaining() < 5 {
            dst.reserve(5);
            return Ok(None);
        }

        let msg = dst[0];
        let len = u32::from_be_bytes(dst[1..5].try_into().unwrap()) as usize;

        if len + 5 > dst.remaining() {
            dst.reserve(len + 5);
            return Ok(None);
        }

        dst.advance(5);
        let mut payload = dst.split_to(len);

        match msg {
            // postgres
            0 => Ok(Some(PglbMessage::Postgres(payload.freeze()))),
            // control
            1 => {
                if payload.is_empty() {
                    bail!("invalid ctrl message")
                }
                let ctrl_msg = payload.split_to(1)[0];
                let ctrl_msg = match ctrl_msg {
                    0 => PglbControlMessage::ConnectionInitiated(
                        serde_json::from_slice(&payload).context("deser")?,
                    ),
                    // ipv4 socket
                    1 if len == 7 => PglbControlMessage::ConnectToCompute {
                        socket: SocketAddr::new(
                            IpAddr::V4(Ipv4Addr::from_bits(payload.get_u32())),
                            payload.get_u16(),
                        ),
                    },

                    // ipv6 socket
                    1 if len == 19 => PglbControlMessage::ConnectToCompute {
                        socket: SocketAddr::new(
                            IpAddr::V6(Ipv6Addr::from_bits(payload.get_u128())),
                            payload.get_u16(),
                        ),
                    },

                    2 if len == 1 => PglbControlMessage::ComputeEstablish,

                    _ => bail!("invalid ctrl message"),
                };
                Ok(Some(PglbMessage::Control(ctrl_msg)))
            }
            _ => bail!("invalid message"),
        }
    }
}

pub enum PglbMessage {
    Control(PglbControlMessage),
    Postgres(bytes::Bytes),
}

pub enum PglbControlMessage {
    // from pglb to auth proxy
    ConnectionInitiated(ConnectionInitiatedPayload),
    // from auth proxy to pglb
    ConnectToCompute { socket: SocketAddr },
    // from auth proxy to pglb
    ComputeEstablish,
}

#[derive(Serialize, Deserialize)]
pub struct ConnectionInitiatedPayload {
    tls_server_end_point: TlsServerEndPoint,
    server_name: Option<String>,
    ip_addr: IpAddr,
}
