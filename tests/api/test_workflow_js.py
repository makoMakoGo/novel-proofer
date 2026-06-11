from __future__ import annotations

import subprocess
import textwrap


def test_frontend_workflow_actions_use_snapshot_commands() -> None:
    script = textwrap.dedent(
        """
        import assert from 'node:assert/strict';
        import {
            actionAvailability,
            primaryActionKey,
            settingsLockState,
            snapshotLabel,
            snapshotTone,
        } from './templates/static/js/workflow.js';

        const readyToProcess = {
            id: 'job1',
            workflow_phase: 'process',
            execution_state: 'idle',
            wait_reason: 'ready_to_process',
            terminal_state: null,
            available_commands: ['process', 'detach', 'reset'],
            progress: { done_chunks: 0, total_chunks: 1 },
        };
        assert.equal(actionAvailability(readyToProcess).canProcess, true);
        assert.equal(primaryActionKey(readyToProcess), 'process');

        const userPaused = {
            ...readyToProcess,
            wait_reason: 'user_paused',
            progress: { done_chunks: 1, total_chunks: 3 },
        };
        assert.equal(actionAvailability(userPaused).canProcess, true);
        assert.equal(actionAvailability(userPaused).canPause, false);
        assert.equal(snapshotLabel(userPaused), '已暂停');
        assert.equal(snapshotTone(userPaused), 'wait');

        const running = {
            id: 'job2',
            workflow_phase: 'process',
            execution_state: 'running',
            wait_reason: null,
            terminal_state: null,
            available_commands: ['pause', 'reset'],
            progress: { done_chunks: 1, total_chunks: 3 },
        };
        assert.equal(actionAvailability(running).canPause, true);
        assert.equal(actionAvailability(running).canProcess, false);
        assert.equal(actionAvailability(running).canHardReset, false);
        assert.equal(primaryActionKey(running), 'pause');

        const readyToMerge = {
            id: 'job3',
            workflow_phase: 'merge',
            execution_state: 'idle',
            wait_reason: 'ready_to_merge',
            terminal_state: null,
            available_commands: ['merge', 'detach', 'reset'],
            progress: { done_chunks: 3, total_chunks: 3 },
        };
        assert.equal(actionAvailability(readyToMerge).canMerge, true);
        assert.equal(primaryActionKey(readyToMerge), 'merge');

        const done = {
            id: 'job4',
            workflow_phase: 'done',
            execution_state: 'idle',
            wait_reason: null,
            terminal_state: 'done',
            available_commands: ['download', 'detach', 'reset'],
            progress: { done_chunks: 3, total_chunks: 3 },
        };
        assert.equal(actionAvailability(done).canDownload, true);
        assert.equal(primaryActionKey(done), 'download');
        assert.equal(snapshotLabel(done), '已完成');
        assert.equal(snapshotTone(done), 'success');

        const cancelled = {
            id: 'job5',
            workflow_phase: 'process',
            execution_state: 'idle',
            wait_reason: null,
            terminal_state: 'cancelled',
            available_commands: [],
            progress: { done_chunks: 0, total_chunks: 3 },
        };
        assert.equal(actionAvailability(cancelled).canDetach, false);
        assert.equal(primaryActionKey(cancelled), null);
        """
    )

    subprocess.run(["node", "--input-type=module", "-e", script], check=True)


def test_frontend_settings_locks_use_snapshot_state() -> None:
    script = textwrap.dedent(
        """
        import assert from 'node:assert/strict';
        import { actionAvailability, settingsLockState } from './templates/static/js/workflow.js';

        const detached = settingsLockState(null);
        assert.equal(detached.formatLocked, false);
        assert.equal(detached.llmLocked, false);

        const readyToProcess = {
            id: 'job1',
            workflow_phase: 'process',
            execution_state: 'idle',
            wait_reason: 'ready_to_process',
            terminal_state: null,
            available_commands: ['process', 'detach', 'reset'],
            progress: { done_chunks: 0, total_chunks: 2 },
        };
        assert.equal(settingsLockState(readyToProcess).formatLocked, true);
        assert.equal(settingsLockState(readyToProcess).llmLocked, false);
        assert.equal(actionAvailability(readyToProcess).canProcess, true);

        const userPaused = {
            ...readyToProcess,
            wait_reason: 'user_paused',
            progress: { done_chunks: 1, total_chunks: 2 },
        };
        assert.equal(settingsLockState(userPaused).formatLocked, true);
        assert.equal(settingsLockState(userPaused).llmLocked, false);

        const running = {
            ...readyToProcess,
            execution_state: 'running',
            wait_reason: null,
            available_commands: ['pause', 'reset'],
        };
        assert.equal(settingsLockState(running).formatLocked, true);
        assert.equal(settingsLockState(running).llmLocked, true);
        assert.equal(actionAvailability(running).canProcess, false);

        const retryableError = {
            ...readyToProcess,
            terminal_state: 'error',
            wait_reason: null,
            available_commands: ['retry_failed', 'detach', 'reset'],
        };
        assert.equal(settingsLockState(retryableError).formatLocked, true);
        assert.equal(settingsLockState(retryableError).llmLocked, false);
        assert.equal(actionAvailability(retryableError).canRetry, true);

        const readyToMerge = {
            ...readyToProcess,
            workflow_phase: 'merge',
            wait_reason: 'ready_to_merge',
            available_commands: ['merge', 'detach', 'reset'],
            progress: { done_chunks: 2, total_chunks: 2 },
        };
        assert.equal(settingsLockState(readyToMerge).formatLocked, true);
        assert.equal(settingsLockState(readyToMerge).llmLocked, false);
        assert.equal(settingsLockState(readyToMerge).formatLockReason.includes('api/'), false);
        assert.equal(settingsLockState(running).llmLockReason.includes('/'), false);
        """
    )

    subprocess.run(["node", "--input-type=module", "-e", script], check=True)
