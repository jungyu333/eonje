---
description: Write unit tests for the given module or function from real API fixtures
argument-hint: [module-or-function, e.g. eonje/transform/sessions.py]
context: fork
agent: test-author
background: false
---
# /write-tests

Write or extend unit tests for: $ARGUMENTS

If no target is given, cover functions changed in `git diff main` that lack tests.
Follow the procedure, required cases, and hard rules in your agent definition.
