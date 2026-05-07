// Workflow state helpers shared by UI rendering and event handlers.

export function normalizeJobState(job) {
    return {
        hasJob: !!job?.id,
        state: String(job?.state || '').toLowerCase(),
        phase: String(job?.phase || '').toLowerCase(),
        doneChunks: Number(job?.progress?.done_chunks || 0),
    };
}

export function isInFlight(stateName) {
    return stateName === 'queued' || stateName === 'running';
}

export function actionAvailability(job, { hasLocalFile = false, createJobInFlight = false } = {}) {
    const view = normalizeJobState(job);
    const inFlight = isInFlight(view.state);
    const canResumeValidate = view.hasJob && view.state === 'paused' && view.phase === 'validate';
    const canStartValidate = !view.hasJob && !!hasLocalFile && !createJobInFlight;

    return {
        ...view,
        inFlight,
        canResumeValidate,
        canStartValidate,
        canValidate: canResumeValidate || canStartValidate,
        isSubmitting: !view.hasJob && !!createJobInFlight,
        canProcess: view.hasJob && view.state === 'paused' && view.phase === 'process',
        canMerge: view.hasJob && view.state === 'paused' && view.phase === 'merge',
        canPause: view.hasJob && inFlight && view.phase === 'process',
        canRetry: view.hasJob && view.state === 'error',
        canDetach: view.hasJob && !inFlight,
        canHardReset: view.hasJob && !(inFlight && view.phase === 'process'),
        canDownload: view.hasJob && view.state === 'done',
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
