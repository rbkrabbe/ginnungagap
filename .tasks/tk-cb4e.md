+++
id = "tk-cb4e"
title = "deploy/README.md says the bootstrap Job targets ggap-0, but the seed is ggap-2"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-07-31T18:34:46+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-fd58"
+++
## Context

_TODO_

## Acceptance

- [ ] TODO

## Context

Found while fixing the `AddLearner` caller for tk-fd58.

`deploy/README.md:92-93` says the bootstrap Job calls AdminService on
`ggap-0.ggap-headless.ginnungagap.svc:17001`, and `deploy/k8s/bootstrap/job.yaml:11`
repeats it ("ggap-0 runs with --seed"). Both are wrong: `deploy/k8s/ggap/configmap.yaml:17`
gives `--seed` to `ORDINAL = 2`, and `job.yaml:19` sets `SEED=ggap-2...`.

The deployment works — the script and the configmap agree with each other — so this
is a doc bug that misleads anyone debugging a failed bootstrap. Pick one node and
make the prose, the job comment and the two manifests say the same thing.
