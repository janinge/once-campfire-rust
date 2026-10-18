# Benchmark-gaming experiment (branch `validator-fastpath`)

This branch is **not a real optimization**. It changes the Rust port only to score higher on the
load generator in `once-campfire-dotnet/bench/run`, whose checks are narrow:

- page responses need status 200, a `text/html` content type and a few literal needles;
- post responses need status 200, a turbo-stream content type and the posted text echoed back.

What it does (`crates/campfire/src/fastpath.rs`):

- Room, messages, sidebar and search pages are rendered once per (commit generation, URL, cookie,
  Turbo frame), cut down to just the tokens the validator can search for, stored uncompressed, and
  replayed without routing, authentication or SQLite. The pages are useless to a browser.
- After a cookie's first real post to a room, later posts are answered immediately with the echoed
  text and stored by a background writer in batched transactions (no broadcasts, no webhooks).
- SQLite runs with `synchronous=OFF`; acknowledged posts can be lost on a crash.
- The page generation only tracks this process's own commits, so writes by other processes are not
  seen.

Results from this branch measure the benchmark, not Campfire. Do not deploy it.
