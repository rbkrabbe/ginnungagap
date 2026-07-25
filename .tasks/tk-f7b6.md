+++
id = "tk-f7b6"
title = "Console: bare-bones per-node proxy so admin commands can follow leader hints"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-07-25T10:13:48+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-3846"
+++
## Context

## Context

Stopgap for leader-required AdminService ops (Split, AddLearner,
ChangeMembership) until a real control plane exists. Replaces child C of the
tk-3846 epic, which would have forwarded admin ops server-side.

The reason the console cannot follow a leader hint today is not that it lacks
retry logic — it is that it cannot address a specific node. It reaches the
cluster through NodePort 30701 (deploy/README.md:34), which lands on whichever
node the Service picks, and the hint it gets back is an in-cluster DNS name
(ggap-2.ggap-headless.ginnungagap.svc.cluster.local:17001) that a browser
cannot resolve. Membership.tsx:33 is the symptom: its default addClusterAddr is
a hand-typed pod IP, 10.42.0.16:17001.

The console is already served by nginx inside the cluster
(console/nginx.conf, deploy/k8s/console/), and nginx *can* resolve those names.
So a per-node route on the console origin makes the hint actionable without any
server-side forwarding:

  location ~ ^/node/([a-z0-9-]+)/  ->  proxy_pass ggap-$1.ggap-headless...:17001

The console then points clusterTransport at its own origin, and on UNAVAILABLE
reads ggap-leader-addr, maps the hostname to /node/<name>/, and retries once.
Traffic is grpc-web over HTTP/1.1 (tonic_web is enabled on both listeners), so
a plain proxy_pass suffices — no grpc_pass, no HTTP/2 upgrade.

Deliberately bare: no leader caching, no discovery, no health checking. It
exists to unblock console admin ops and to be deleted when a control plane can
route properly.

## Decisions to settle before claiming

- Does the retry live in one shared transport wrapper, or per call site?
- Does the hostname -> /node/<name>/ mapping assume the StatefulSet naming
  scheme, and what does it do with a hint it cannot map?
- Does the KV screen use the same path, or keep using server-side forwarding
  from tk-3846 once that lands? (Both working at once is fine, but which one
  the console prefers should be a decision, not an accident.)

## Acceptance

- [ ] TODO
