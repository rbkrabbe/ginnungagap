+++
id = "tk-0bd3"
title = "sim_cluster::test_partition_and_heal is flaky on CI"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-07-31T17:59:37+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-441f"
+++
## Context

Observed on PR #65 (tk-441f). Two CI runs on the branch: `a11fb96` (the code
commit) passed; `6958db9` — which changes **only** `.tasks/tk-441f.md`, not a
line of Rust — failed:

```
crates/ggap-consensus/tests/sim_cluster.rs:570:5:
assertion `left == right` failed: isolated node should replicate after partition heals
  left: None
 right: Some([109, 97, 106, 111, 114, 105, 116, 121])   // b"majority"
```

Re-running the failed job on that identical SHA passed. Same code, both verdicts —
so this is the test, not the change under review. 15 consecutive local runs also
passed, which fits a load-sensitive failure that a shared CI runner reproduces and
a quiet laptop does not.

The suspected mechanism is in the DST harness rather than in consensus: these
tests pause time and step it forward, then `drain_tasks(n)` yields a fixed number
of times to let woken tasks run. A fixed yield count is a guess about how much
work the scheduler will complete, and on a loaded runner it can be too few — the
isolated node's replication has been scheduled but has not run by the time the
assertion reads the FSM. If that is right, the fix is to replace the fixed drain
with a bounded wait on the condition (poll the FSM until it converges or a
deadline in *simulated* time expires), not to raise the yield count, which only
moves the threshold.

Worth confirming before fixing: instrument the failing assertion to print
`raft.metrics()` for all three nodes, and check whether the other six tests in
the file share the pattern — `test_partition_and_heal` may just be the one with
the tightest margin, in which case fixing it alone leaves the rest latent.

A flaky test in the one suite that exists to make consensus deterministic is worth
more than its size suggests: it trains everyone to re-run CI, which is exactly the
habit that hides a real regression.

## Acceptance

- [ ] The mechanism is identified and stated in the task — not "added a retry".
- [ ] The failing assertion waits on a condition rather than on a fixed number of
      yields, or the task explains why a fixed count is correct here.
- [ ] The other six tests in `sim_cluster.rs` are checked for the same pattern and
      either fixed with it or explicitly cleared.
- [ ] 50 consecutive runs of the whole `sim_cluster` suite pass locally
      (`for i in $(seq 50)`), and the suite passes on CI under `--test-threads` set
      high enough to reproduce contention.
