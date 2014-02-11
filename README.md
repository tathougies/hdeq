HDeq: Haskell Distributed Event Queue
=====================================

HDeq is a distributed event queue for Haskell. This means it allows for the creation of named queues
which can be shared amongst multiple hosts, and which can be used to stream any Haskell data type
which is an instance of `Serialize` from the `cereal` package.

HDeq was created for the use of financial data streaming, but it can be adapted to any sort of
problem which may involve broadcasting of large data streams to multiple subscribers.

HDeq leverages Haskell's unique multithreading and concurrency abilities, combined with its purity
and type safety to provide maximal uptime, reliability, and efficiency.

Current State
-------------

HDeq currently provides:

 * A simple and unified API for creating and managing distributed queues.

Future Plans
------------

In the future, HDeq will provide:

 * A well-thought-out access control system
 * Throttling and bandwidth limiting support
 * Latency handlers

Benchmarks
-----------

TBD...
