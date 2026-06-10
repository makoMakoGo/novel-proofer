// Workflow state helpers shared by UI rendering and event handlers.

export function normalizeJobState(job) {
    const commands = Array.isArray(job?.available_commands) ? job.available_commands.map((v) => String(v)) : [];
    return {
        hasJob: !!job?.id,
        workflowPhase: String(job?.workflow_phase || '').toLowerCase(),
        executionState: String(job?.execution_state || '').toLowerCase(),
        waitReason: job?.wait_reason == null ? null : String(job.wait_reason).toLowerCase(),
        terminalState: job?.terminal_state == null ? null : String(job.terminal_state).toLowerCase(),
        commands: new Set(commands),
        doneChunks: Number(job?.progress?.done_chunks || 0),
    };
}

export function isInFlight(executionState) {
    return executionState === 'queued' || executionState === 'running';
}

export function actionAvailability(job, { hasLocalFile = false, createJobInFlight = false } = {}) {
    const view = normalizeJobState(job);
    const inFlight = isInFlight(view.executionState);
    const canResumeValidate = view.hasJob && view.commands.has('validate');
    const canStartValidate = !view.hasJob && !!hasLocalFile && !createJobInFlight;

    return {
        ...view,
        inFlight,
        canResumeValidate,
        canStartValidate,
        canValidate: canResumeValidate || canStartValidate,
        isSubmitting: !view.hasJob && !!createJobInFlight,
        canProcess: view.hasJob && view.commands.has('process'),
        canMerge: view.hasJob && view.commands.has('merge'),
        canPause: view.hasJob && view.commands.has('pause'),
        canRetry: view.hasJob && view.commands.has('retry_failed'),
        canDetach: view.hasJob && view.commands.has('detach'),
        canHardReset: view.hasJob && view.commands.has('reset') && !(inFlight && view.workflowPhase === 'process'),
        canDownload: view.hasJob && view.commands.has('download'),
    };
}

export function primaryActionKey(job, options = {}) {
    const availability = actionAvailability(job, options);
    if (!availability.hasJob || availability.canResumeValidate) return 'validate';
    if (availability.canProcess) return 'process';
    if (availability.canMerge) return 'merge';
    if (availability.canDownload) return 'download';
    if (availability.canPause) return 'pause';
    if (availability.canRetry) return 'retry';
    return null;
}
