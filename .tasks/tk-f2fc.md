+++
id = "tk-f2fc"
title = "Move NodeAddrs into ggap-types"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-07-30T19:07:52+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-d08a"
+++
## Context

Pure refactor. No behaviour, no format change, no openraft edit — it exists so
the next two children have one shared address type to reach for instead of
inventing two.

`NodeAddrs` currently lives in `ggap-consensus/src/registry.rs`. `GgapNode` will
need its shape, and so will `KvCommand::Split.source_members` (tk-10b7) — and
that lives in `ggap-types`, which has no openraft dependency and must keep none
(Cargo.toml lists only thiserror, serde, bytes, bincode). So the domain type
belongs in `ggap-types` and the openraft impl wraps or converts from it.

Move it, re-export from `ggap-consensus` so existing imports keep working, and
leave every call site otherwise untouched.

## Acceptance

- [ ] `NodeAddrs` is defined in `ggap-types`; `ggap-consensus` re-exports it.
- [ ] `ggap-types` still depends on no openraft and no gRPC crate.
- [ ] No call site changed beyond its import path.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
