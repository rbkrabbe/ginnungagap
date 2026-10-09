+++
id = "tk-ad4e"
title = "CI tracks rustup stable, so a new clippy breaks every code PR on generated proto code"
kind = "task"
status = "done"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-09T18:44:28+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-92e0"
resized_from = "m"
base = "05c02c0f13ab3d6f09aba19e20deacb95be32642"
closed = "2026-10-09T18:52:52+00:00"
+++
## Context

`.github/workflows/ci.yml` installed Rust with `rustup update stable`, so CI
followed whatever stable happened to be on the runner that day while
developers ran whatever they had locally. Under `-D warnings` that makes the
four-check list in CLAUDE.md unable to predict CI: every clippy release adds
lints, and a lint added after the last local `rustup update` is a build
failure nobody can see before pushing.

It fired on tk-5a4b (PR #86), a comments-only change: CI clippy 0.1.99
reported **17 `double_must_use` errors** in `ggap-proto`'s generated
`ginnungagap.v1.rs`, where `#[async_trait]` wraps a trait whose return type
prost and tonic have already marked `#[must_use]`. Local clippy 0.1.93 knows
no such lint. Nothing in this repo authored that code, so there is nothing to
fix in it — and `#[allow]` cannot be placed inside generated output.

Every code-touching PR was red for the same reason, independent of content.

## Approach

`rust-toolchain.toml` pins `channel = "1.93.1"` with `rustfmt` and `clippy`,
and CI installs what the file names rather than naming a version itself. One
source for the toolchain, honoured by `cargo` for everyone.

Rejected: suppressing the lint. It would have to be suppressed at the crate
root of `ggap-proto` (`#![allow(clippy::double_must_use)]`), which silences
the lint for hand-written code in that crate too, and buys nothing the next
new lint will not undo.

## Acceptance

- [x] CI and a local run use the same toolchain, named in one place.
- [x] Full checklist green under the pin: fmt, clippy -D warnings, build,
      test (25 result lines, 0 failures).
- [x] A code-touching PR goes green on CI — this one, 3m21s, Rust steps
      run (the diff touches `ci.yml`, so the skip path did not apply).
