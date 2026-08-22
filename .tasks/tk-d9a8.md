+++
id = "tk-d9a8"
title = "split_crash_bugs never runs: neither the checklist nor CI enables test-utils"
kind = "task"
status = "done"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-08-22T07:01:03+00:00"
spec_approved = false
review = "none"
touched = []
base = "01477fc53e3c90be6b6401a471f2daa1f8663285"
closed = "2026-08-22T08:39:03+00:00"
+++
## Context

The split crash-atomicity suite is dead weight. `crates/ggap-storage/Cargo.toml`
gates it:

    [[test]]
    name = "split_crash_bugs"
    required-features = ["test-utils"]

and nothing passes that feature. The CLAUDE.md checklist runs `cargo test --all`;
CI (`.github/workflows/ci.yml`) runs `cargo test --all`. Neither enables
`test-utils`, so cargo skips the target silently — no skip line, no warning, an
all-green run.

CI is worse than the checklist: its clippy step is `cargo clippy --all-targets`
while the checklist says `cargo clippy --all-targets --all-features`. So the
crash tests at least *compile* locally, and in CI are never even type-checked.
That drift between the two clippy lines is its own small bug.

This is the coverage for `apply`'s split path — the one place where data
movement, `last_applied`, two shard map records and `bootstrap_members` commit
in a single batch that must be all-or-nothing across a crash. tk-239e moves
shard map records between keyspaces and touches exactly that batch, which is
what surfaced this.

`test-utils` is the only feature in the workspace, so `--all-features` enables
it and nothing else.

## Acceptance

- [x] `split_crash_bugs` actually runs in CI, and a deliberately broken split
      batch fails the run — verify by breaking it locally, not by reading the
      log.
- [x] The CLAUDE.md checklist runs it too, so a push cannot be greener than CI.
- [x] The checklist and CI clippy lines stop disagreeing about `--all-features`.
- [x] `test-utils` stays out of production builds; the fix is in how tests are
      invoked, not in making the feature default.
