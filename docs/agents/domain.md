# Domain Docs

This is a single-context repo for Novel Proofer, a local Chinese novel formatting/proofreading tool.

## Read First

- `openspec/project.md` for project conventions and domain constraints.
- Relevant current specs under `openspec/specs/`.
- Active changes under `openspec/changes/` before proposing architecture or behavior changes.
- `docs/ARCHITECTURE.md`, `docs/STATE_MACHINE.md`, and `docs/WORKFLOW.md` for implemented workflow and recovery behavior.
- `CLAUDE.md` and `AGENTS.md` for setup, validation, and repo-specific development rules.

If `CONTEXT.md`, `CONTEXT-MAP.md`, or `docs/adr/` are added later, read the relevant files before architecture or implementation work.

## Vocabulary

Use the project's existing domain terms in issues and PRDs:

- Job: a persistent proofreading task.
- Chunk: a line-bounded unit processed independently.
- Phase: the workflow stage, currently `validate`, `process`, `merge`, or `done`.
- Job state: the visible lifecycle state, currently `queued`, `running`, `paused`, `done`, `error`, or `cancelled`.
- UI attach: the browser-side association with a job id.
- Reset: the hard delete/cleanup action for a job.
