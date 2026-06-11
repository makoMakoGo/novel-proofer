// Browser-side job attachment helpers.

export const ATTACHED_JOB_KEY = 'novel_proofer.attached_job_id.v2';

export function normalizeAttachedJobId(value) {
    return String(value || '').trim();
}

export function readPersistedAttachment(storage) {
    const jobId = normalizeAttachedJobId(storage.getItem(ATTACHED_JOB_KEY));
    return jobId || null;
}

export function persistAttachment(storage, jobId) {
    const normalized = normalizeAttachedJobId(jobId);
    if (!normalized) throw new Error('missing job_id');
    storage.setItem(ATTACHED_JOB_KEY, normalized);
    return normalized;
}

export function clearPersistedAttachment(storage) {
    storage.removeItem(ATTACHED_JOB_KEY);
}
