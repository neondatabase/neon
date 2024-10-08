"""
Pagebench-based performance regression tests.

The defining characteristic of tests in this sub-directory is that they
are component-level tests, i.e., they exercise pageserver directly using `pagebench`
instead of benchmarking the full stack.

See https://github.com/neondatabase/neon/issues/5771
for the context in which this was developed.
"""

from __future__ import annotations
