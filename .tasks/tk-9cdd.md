+++
id = "tk-9cdd"
title = "A boot counter write that fails transiently now refuses the start with no retry"
kind = "task"
status = "dropped"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-09T19:39:24+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-d4b8"
resized_from = "m"
+++
## Context

tk-d4b8 made `BootCounter::advance` fail when it cannot persist the rank,
because a rank the next boot would reissue is one an address change could
never install past. The refusal is on the first failed write, with no retry.

Raised while deciding tk-d4b8 and deliberately left open: whether a write
failure here is ever transient enough to be worth retrying before giving up.
Unresolved because nothing in the code answers it — `fjall`'s error type is
flattened to `GgapError::Storage(String)` at
`boot_counter.rs:154-159`, so the one `u64` write cannot currently
distinguish a full disk from a momentary I/O error.

The posture argument for refusing immediately is that this write shares a disk
with the Raft log, so a node that cannot do it was not going to serve well
anyway. That holds for a persistent fault and not for a blip.

Worth answering only with evidence: if `fjall` surfaces a retryable error
class, a bounded retry costs little; if it does not, the current behaviour is
already the right one and this task closes as a no-op with that recorded.

## Acceptance

- [ ] Established whether `fjall` distinguishes a retryable write failure
      from a terminal one, in the version pinned here.
- [ ] Either a bounded retry before the refusal, or this task closed with the
      finding that there is nothing to retry on.

## Dropped (2026-10-11)

Answered in one grep rather than researched, which is the sign it should never
have been a task.

`fjall::Error` (`fjall-3.0.3/src/error.rs:12`) has no retryable class.
`Poisoned` carries fjall's own instruction — *"At this point, it's best to let
the application crash and try to recover"* — and the rest are format, journal
or decompression failures. The only candidate is `Io(std::io::Error)`, and a
failed write of eight bytes onto the same disk as the Raft log is not a
transient class worth looping on.

And the argument that settles it without reference to fjall at all: **the
supervisor is the retry.** tk-4822 established that an exit under a
StatefulSet is a restart, so a refused boot is already retried automatically
with backoff. An in-process retry adds nothing a restart does not, and a
crash loop is the more honest signal for a disk that cannot take a `u64`.

The lesson is about the filing, not the answer: a task whose deliverable is
knowledge I can obtain in thirty seconds is a lookup I declined to do.
