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

export function snapshotLabel(job) {
    const view = normalizeJobState(job);
    if (!view.hasJob) return '-';

    if (view.terminalState) {
        const labels = { done: '已完成', error: '出错', cancelled: '已删除' };
        return labels[view.terminalState] || `未知终态:${view.terminalState}`;
    }
    if (view.executionState === 'queued') return '排队中';
    if (view.executionState === 'running') return '运行中';
    if (view.executionState !== 'idle') return `未知执行态:${view.executionState || '-'}`;
    if (!view.waitReason) return '空闲';

    const waitLabels = {
        ready_to_process: '等待开始校对',
        user_paused: '已暂停',
        ready_to_merge: '等待合并输出',
        server_recovered: '服务重启后等待继续',
    };
    return waitLabels[view.waitReason] || `未知等待原因:${view.waitReason}`;
}

export function snapshotTone(job) {
    const view = normalizeJobState(job);
    if (!view.hasJob) return 'neutral';
    if (view.waitReason) return 'wait';
    if (view.terminalState === 'error') return 'error';
    if (view.terminalState === 'done') return 'success';
    if (view.executionState === 'queued' || view.executionState === 'running') return 'active';
    return 'neutral';
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

export function settingsLockState(job) {
    const view = normalizeJobState(job);
    const formatLocked = view.hasJob && ['process', 'merge', 'done'].includes(view.workflowPhase);
    const llmLocked = view.hasJob && isInFlight(view.executionState);

    return {
        ...view,
        formatLocked,
        llmLocked,
        formatLockReason: formatLocked
            ? '任务已完成预处理，切片与格式设置已写入当前任务。'
            : '',
        llmLockReason: llmLocked
            ? '任务正在执行，LLM 设置会在暂停、出错或等待下一步时解锁。'
            : '',
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
