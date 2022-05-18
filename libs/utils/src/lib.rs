//! `utils` is intended to be a place to put code that is shared
//! between other crates in this repository.

#![allow(clippy::manual_range_contains)]

/// `Lsn` type implements common tasks on Log Sequence Numbers
pub mod lsn;
/// SeqWait allows waiting for a future sequence number to arrive
pub mod seqwait;

/// append only ordered map implemented with a Vec
pub mod vec_map;

// Async version of SeqWait. Currently unused.
// pub mod seqwait_async;

pub mod bin_ser;
pub mod postgres_backend;
pub mod pq_proto;

// dealing with connstring parsing and handy access to it's parts
pub mod connstring;

// helper functions for creating and fsyncing directories/trees
pub mod crashsafe_dir;

// common authentication routines
pub mod auth;

// utility functions and helper traits for unified unique id generation/serialization etc.
pub mod zid;
// http endpoint utils
pub mod http;

// socket splitting utils
pub mod sock_split;

// common log initialisation routine
pub mod logging;

// Misc
pub mod accum;
pub mod shutdown;

// Tools for calling certain async methods in sync contexts
pub mod sync;

// Utility for binding TcpListeners with proper socket options.
pub mod tcp_listener;

// Utility for putting a raw file descriptor into non-blocking mode
pub mod nonblock;

// Default signal handling
pub mod signals;

/// This is a shortcut to embed git sha into binaries and avoid copying the same build script to all packages
///
/// we have several cases:
/// * building locally from git repo
/// * building in CI from git repo
/// * building in docker (either in CI or locally)
///
/// One thing to note is that .git is not available in docker (and it is bad to include it there).
/// So everything becides docker build is covered by git_version crate, and docker uses a `GIT_VERSION` argument to get the value required.
/// It takes variable from build process env and puts it to the rustc env. And then we can retrieve it here by using env! macro.
/// Git version received from environment variable used as a fallback in git_version invokation.
/// And to avoid running buildscript every recompilation, we use rerun-if-env-changed option.
/// So the build script will be run only when GIT_VERSION envvar has changed.
///
/// Why not to use buildscript to get git commit sha directly without procmacro from different crate?
/// Caching and workspaces complicates that. In case `utils` is not
/// recompiled due to caching then version may become outdated.
/// git_version crate handles that case by introducing a dependency on .git internals via include_bytes! macro,
/// so if we changed the index state git_version will pick that up and rerun the macro.
///
/// Note that with git_version prefix is `git:` and in case of git version from env its `git-env:`.
///
/// #############################################################################################
/// TODO this macro is not the way the library is intended to be used, see https://github.com/neondatabase/neon/issues/1565 for details.
/// We use `cachepot` to reduce our current CI build times: https://github.com/neondatabase/cloud/pull/1033#issuecomment-1100935036
/// Yet, it seems to ignore the GIT_VERSION env variable, passed to Docker build, even with build.rs that contains
/// `println!("cargo:rerun-if-env-changed=GIT_VERSION");` code for cachepot cache invalidation.
/// The problem needs further investigation and regular `const` declaration instead of a macro.
#[macro_export]
macro_rules! project_git_version {
    ($const_identifier:ident) => {
        const $const_identifier: &str = git_version::git_version!(
            prefix = "git:",
            fallback = concat!(
                "git-env:",
                env!("GIT_VERSION", "Missing GIT_VERSION envvar")
            ),
            args = ["--abbrev=40", "--always", "--dirty=-modified"] // always use full sha
        );
    };
}

/// Same as `assert!`, but evaluated during compilation and gets optimized out in runtime.
#[macro_export]
macro_rules! const_assert {
    ($($args:tt)*) => {
        const _: () = assert!($($args)*);
    };
}
