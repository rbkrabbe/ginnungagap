+++
id = "tk-f2fc"
title = "Move NodeAddrs into ggap-types"
kind = "task"
status = "in_progress"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-07-30T19:07:52+00:00"
spec_approved = false
review = "none"
touched = ["crates/ggap-types/src/lib.rs", "crates/ggap-consensus/src/registry.rs", "crates/ggap-consensus/src/lib.rs", "crates/ggap-consensus/src/gossip.rs"]
parent = "tk-d08a"
base = "80cd06ba9ecf1b2ca05793354c361c6ef5c1a74a"
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

- [x] `NodeAddrs` is defined in `ggap-types`; `ggap-consensus` re-exports it.
- [x] `ggap-types` still depends on no openraft and no gRPC crate.
      (`cargo tree -p ggap-types`: bincode, bytes, serde, thiserror only.)
- [x] No call site changed beyond its import path. Only `gossip.rs` needed one,
      because it imported through `crate::registry` rather than the crate root.
- [x] Full checklist green: fmt, clippy -D warnings, build, test.

## Outcome

Four files, no behaviour change: +36 in `ggap-types`, −35 in `registry.rs`, an
import in `gossip.rs`, a re-export in `lib.rs`.

Gained `serde::Serialize`/`Deserialize` in the move. Not scope creep — every
domain type in `ggap-types` derives them, and tk-10b7 needs them to put
`NodeAddrs` inside `KvCommand::Split`. Nothing serializes the type yet, so no
format changed here.

The doc comment was rewritten rather than moved. It pointed at
`ShardRegistry::merge_directory` to explain the field-wise merge rule, which
would have been a broken intra-doc link from a crate that cannot see the
registry — and the wrong place for it regardless. The type now describes itself
and states that merge policy belongs to whoever holds it. That matters for
tk-11b6, which is expected to change that policy.

**Re-export vs. one canonical path.** `ggap-consensus` re-exports only its own
types elsewhere, so `pub use ggap_types::NodeAddrs` breaks that convention and
leaves two valid import paths. Kept it because the alternative — updating all
seven `ggap_consensus::NodeAddrs` importers — would have made this a 10-file
change and forced a resize off `s`, for a task whose entire point is to be
invisible. The re-export carries a comment pointing new code at `ggap_types`.
Worth deleting once tk-441f and tk-11b6 have churned those files anyway — filed
as **tk-3080**, blocked on tk-11b6, with instructions to fold it into whichever
task edits those imports first rather than spend a PR on `use` statements.
