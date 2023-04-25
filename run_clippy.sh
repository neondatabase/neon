#!/bin/bash

# If you save this in your path under the name "cargo-zclippy" (or whatever
# name you like), then you can run it as "cargo zclippy" from the shell prompt.
#
# If your text editor has rust-analyzer integration, you can also use this new
# command as a replacement for "cargo check" or "cargo clippy" and see clippy
# warnings and errors right in the editor.
# In vscode, this setting is Rust-analyzer>Check On Save:Command

# * `-A unknown_lints` – do not warn about unknown lint suppressions
#                        that people with newer toolchains might use
# * `-D warnings`      - fail on any warnings (`cargo` returns non-zero exit status)

# NB: the CI runs the full feature powerset, so, it catches slightly more errors
# at the expense of longer runtime. This script is used by developers, so, don't
# do that here.
# NB: keep the args after the `--` in sync with the CI YAMLs.
cargo clippy --locked --all --all-targets --all-features -- -A unknown_lints -D warnings
