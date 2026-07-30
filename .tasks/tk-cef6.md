+++
id = "tk-cef6"
title = "commit_gate exempts .tasks/*.md but tk treats all of .tasks/ as bookkeeping"
kind = "task"
status = "open"
size = "s"
priority = 3
blocked_by = []
tags = []
created = "2026-07-30T19:20:48+00:00"
spec_approved = false
review = "none"
touched = []
+++
## Context

Two hooks draw the bookkeeping line in different places, and `bin/tk`'s own docstring claims they agree.

`is_bookkeeping` (bin/tk:515-527) returns true for anything under `.tasks/`, so `.tasks/config.toml` costs nothing against a task's scope limit. Its docstring says: 'commit_gate draws the line in the same place when it exempts commits of nothing but .tasks/*.md.'

It does not. `staged_is_task_only` (.claude/hooks/commit_gate.py) uses `fnmatch(p, '.tasks/*.md')`, which excludes `config.toml`. So a commit touching only `.tasks/config.toml` — changing scope limits, guarded globs, or the review policy — is bookkeeping for scope but requires a claimed task to commit.

Found while raising the scope limits: the config edit itself could not be committed without claiming something.

Neither behaviour is obviously wrong. Committing a policy change under a claimed task is arguably right, since it is a decision rather than filing. But the docstring asserting parity is wrong either way, and if the intent is parity the fix is one glob.

Decide which line is correct, then make the code and the docstring agree.

## Acceptance

- [ ] TODO
