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
cargo clippy "${@:2}" --all-targets --all-features --all --tests -- -A unknown_lints -D warnings
