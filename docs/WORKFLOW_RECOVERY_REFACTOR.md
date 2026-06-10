# Workflow Recovery Refactor

This document records the approved vocabulary and architectural direction for the breaking workflow recovery refactor tracked by GitHub issues #74-#84.

The goal is to make refresh, close, restart, pause, retry, merge, and reset behavior explicit. The backend owns durable jobs. The browser owns only its current attachment. Worker execution is volatile and belongs to the current backend process.

## Problem

The current implementation overloads `job.state`, especially `paused`, with several meanings:

- validation finished and the job is ready to process;
- the user explicitly paused processing;
- processing finished and the job is ready to merge;
- the server restarted after a job used to be queued or running;
- the UI is detached but the backend job still exists.

The previous frontend attempted a best-effort pause during `pagehide` and `beforeunload`, while localStorage reattached the last job on reload. That made refresh behavior nondeterministic: the same refresh could pause the job if the unload request arrived, or leave it running if it did not.

## Decision

The refactor is a breaking cleanup. Do not preserve old compatibility shims, silent fallback behavior, fake success paths, mock execution paths, or unknown-state downgrades. Invalid states, failed migrations, missing artifacts, and illegal commands must surface clearly.

The model has three separate concepts.

## JobRecord

`JobRecord` is the durable task record. It survives page refresh, browser close, and backend restart.

It owns:

- job id;
- workflow phase;
- durable wait reason;
- input, output, and debug artifact references;
- format snapshot;
- last or intended LLM metadata;
- chunk statuses and counts;
- timestamps;
- diagnostics;
- cleanup policy.

It does not own thread handles, futures, worker identity, in-flight request objects, or process-local callbacks.

## JobExecution

`JobExecution` is the volatile in-process execution attempt. It exists only in the current backend process.

It owns:

- job id;
- command kind, such as validate, process, retry failed chunks, or merge;
- execution state, such as queued, running, or stopping;
- stop or delete request state;
- worker future/callback registration.

`JobExecution` is never treated as durable. After backend restart, the execution registry is empty.

## UiAttachment

`UiAttachment` is the browser-side association with a job id. It may live in localStorage so the page can reattach after refresh.

It owns:

- current attached job id;
- active UI tab;
- frontend-only caches and view state.

It does not own backend job lifecycle. Browser refresh, close, hidden, or navigation events must not call pause, reset, abort, cancel, beacon mutation, keepalive mutation, or any equivalent state-changing endpoint.

## Snapshot Fields

The API snapshot should expose enough information for the UI to render without guessing from `paused`.

Implemented snapshot fields:

- `workflow_phase`: where the durable workflow is, such as validate, process, merge, or done.
- `execution_state`: whether this backend process currently has an execution attempt for the job.
- `wait_reason`: why a durable job is idle.
- `terminal_state`: whether the job has reached done, error, or cancelled.
- `available_commands`: the explicit commands the UI may present.

Recommended wait reasons:

- `ready_to_process`: validation completed and the job is waiting for processing.
- `user_paused`: the user explicitly paused processing.
- `ready_to_merge`: processing completed and the job is waiting for merge.
- `server_recovered`: the backend restarted after a previously active job, and no execution is currently running.

The UI must consume these fields directly instead of reconstructing behavior from the internal persisted `state` and `phase`.

## Browser Lifecycle

Browser lifecycle events are observer-only:

- Refresh does not pause or abort the job.
- Closing the page does not pause or abort the job.
- Reopening the page may reattach to the last job id and fetch the latest snapshot.
- Manual Load may attach to any persisted job.
- Starting a new task detaches the UI from the old job without mutating that job.

Stopping work requires an explicit user command, such as Pause or Reset.

## Restart Recovery

Backend restart recovery must be honest:

- no fake running jobs;
- no automatic resume;
- no silent fallback to a generic paused state.

If a durable record says work was in flight but the process has no execution, the job becomes idle with a server-recovered wait reason or an equivalent explicit state. Chunk states that were `processing` or `retrying` become `pending`, so the user can explicitly resume or retry.

## Command Model

All state-changing actions must go through explicit commands:

- start or continue processing;
- pause;
- retry failed chunks;
- merge;
- detach/new task;
- reset/delete.

Command legality belongs in a pure workflow transition module. API handlers, runner code, and UI action availability should consume that shared decision instead of each hand-writing state rules.

Illegal commands must produce explicit rejections. They must not no-op silently.

## Issue Sequence

The implementation is split into vertical GitHub issues:

- #75 confirms this breaking vocabulary.
- #76 exposes the clean job snapshot contract.
- #77 removes browser lifecycle job mutations.
- #78 adds the pure workflow transition module.
- #79 persists JobRecord and honest restart recovery.
- #80 separates the execution registry from JobRecord.
- #81 rebuilds Load and automatic UI reattachment.
- #82 renders settings locks from the snapshot.
- #83 moves retry and merge onto the command model.
- #84 removes stale docs/contracts and finalizes validation.

Each implementation issue should move one narrow end-to-end behavior through schema, API, UI, and tests where applicable. Avoid broad horizontal rewrites that leave the product between two models.
