# Discrete Event SIMulator

This is a library for running simulations of distributed systems. The main idea is borrowed from [FoundationDB](https://www.youtube.com/watch?v=4fFDFbi3toc).

Each node runs as a separate thread. This library was not optimized for speed yet, but it's already much faster than running usual intergration tests in real time, because it uses virtual simulation time and can fast-forward time to skip intervals where all nodes are doing nothing but sleeping or waiting for something.

The original purpose for this library is to test walproposer and safekeeper implementation working together, in a scenarios close to the real world environment. This simulator is determenistic and can inject failures in networking without waiting minutes of wall-time to trigger timeout, which makes it easier to find bugs in our consensus implementation compared to using integration tests.
