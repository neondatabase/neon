# How to contribute

Howdy! Usual good software engineering practices apply. Write
tests. Write comments. Follow standard Rust coding practices where
possible. Use 'cargo fmt' and 'clippy' to tidy up formatting.

There are soft spots in the code, which could use cleanup,
refactoring, additional comments, and so forth. Let's try to raise the
bar, and clean things up as we go. Try to leave code in a better shape
than it was before.

## Submitting changes

1. Make a PR for every change.

   Even seemingly trivial patches can break things in surprising ways.
Use of common sense is OK. If you're only fixing a typo in a comment,
it's probably fine to just push it. But if in doubt, open a PR.

2. Get at least one +1 on your PR before you push.

   For simple patches, it will only take a minute for someone to review
it.

3. Always keep the CI green.

   Do not push, if the CI failed on your PR. Even if you think it's not
your patch's fault. Help to fix the root cause if something else has
broken the CI, before pushing.

*Happy Hacking!*
