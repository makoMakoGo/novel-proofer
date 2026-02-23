
// API interaction layer

/**
 * Wrapper for fetch to handle JSON responses and errors.
 * @returns {Promise<{ok: boolean, data?: any, error?: any, status?: number}>}
 */
async function apiCall(url, options = {}) {
    try {
        const res = await fetch(url, options);
        const data = await res.json().catch(() => null);
        if (!res.ok) {
            return { ok: false, error: data, status: res.status };
        }
        return { ok: true, data, status: res.status };
    } catch (e) {
        return { ok: false, error: String(e) };
    }
}

export async function fetchLlmSettings() {
    return apiCall('api/v1/settings/llm');
}

export async function saveLlmSettings(payload) {
    return apiCall('api/v1/settings/llm', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
}

export async function createJob(formData) {
    return apiCall('api/v1/jobs', {
        method: 'POST',
        body: formData,
    });
}

export async function pauseJob(jobId) {
    if (!jobId) return { ok: false, error: 'missing job_id' };
    return apiCall(`api/v1/jobs/${encodeURIComponent(jobId)}/pause`, { method: 'POST' });
}

export async function resumeJob(jobId, payload) {
    if (!jobId) return { ok: false, error: 'missing job_id' };
    return apiCall(`api/v1/jobs/${encodeURIComponent(jobId)}/resume`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
}

export async function retryFailedJob(jobId, payload) {
    if (!jobId) return { ok: false, error: 'missing job_id' };
    return apiCall(`api/v1/jobs/${encodeURIComponent(jobId)}/retry-failed`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
}

export async function mergeJob(jobId, payload) {
    if (!jobId) return { ok: false, error: 'missing job_id' };
    return apiCall(`api/v1/jobs/${encodeURIComponent(jobId)}/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
}

export async function resetJob(jobId) {
    if (!jobId) return { ok: false, error: 'missing job_id' };
    return apiCall(`api/v1/jobs/${encodeURIComponent(jobId)}/reset`, { method: 'POST' });
}

export async function fetchJobSummary(jobId) {
    if (!jobId) return { ok: false, error: 'missing job_id' };
    const q = new URLSearchParams({ chunks: '0' });
    return apiCall(`api/v1/jobs/${encodeURIComponent(jobId)}?${q.toString()}`);
}

export async function fetchJobList(limit = 50) {
    return apiCall(`api/v1/jobs?limit=${limit}`);
}

export async function fetchJobInputStats(jobId) {
    if (!jobId) return { ok: false, error: 'missing job_id' };
    return apiCall(`api/v1/jobs/${encodeURIComponent(jobId)}/input-stats`);
}

export async function fetchJobChunks(jobId, filter = 'all', limit = 500, offset = 0) {
    if (!jobId) return { ok: false, error: 'missing job_id' };
    const q = new URLSearchParams({ chunks: '1' });
    q.set('chunk_state', filter);
    q.set('limit', String(limit));
    q.set('offset', String(offset));
    return apiCall(`api/v1/jobs/${encodeURIComponent(jobId)}?${q.toString()}`);
}

export function downloadJobOutput(jobId) {
    if (!jobId) return;
    const url = `api/v1/jobs/${encodeURIComponent(jobId)}/download`;
    const a = document.createElement('a');
    a.href = url;
    a.download = '';
    document.body.appendChild(a);
    a.click();
    a.remove();
}

export async function purgeAllJobs({ exclude = [] } = {}) {
    return apiCall('api/v1/jobs/purge-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ exclude }),
    });
}

/**
 * Best effort pause on exit.
 */
export function bestEffortPauseJob(jobId) {
    const jid = String(jobId || '').trim();
    if (!jid) return;
    const url = `api/v1/jobs/${encodeURIComponent(jid)}/pause`;
    try {
        if (navigator.sendBeacon) {
            navigator.sendBeacon(url, '');
            return;
        }
    } catch (e) {}
    try {
        fetch(url, { method: 'POST', keepalive: true });
    } catch (e) {}
}
