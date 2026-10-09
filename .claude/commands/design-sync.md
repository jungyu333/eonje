---
description: Compare the branch against docs/design.md and update the doc or report code drift
context: fork
agent: design-sync
background: false
---
# /design-sync

Compare the current branch (`git diff main`) against `docs/design.md` and `docs/requirements.md`.

Update the design doc where it is stale. Report code that contradicts a documented decision.
Follow the procedure and hard rules in your agent definition. $ARGUMENTS
