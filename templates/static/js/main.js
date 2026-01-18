import {
    state,
    loadUiState,
    saveUiState,
    getAttachedJobId,
    setAttachedJobId,
    UI_STATE_FIELDS,
    getSavedActiveTab,
    saveActiveTab
} from './state.js';
import * as api from './api.js';
import * as ui from './ui.js';
import * as modal from './modal.js';

// --- Logic Helpers ---

async function ensureJobInputChars(jobId) {
    const jid = String(jobId || '').trim();
    if (!jid) return;
    if (state.inputCharsCache.has(jid)) return;
    if (state.inputCharsInFlight.has(jid)) return;
    state.inputCharsInFlight.add(jid);
    
    try {
        const res = await api.fetchJobInputStats(jid);
        if (!res.ok) {
            const marker = (res.status === 404) ? state.INPUT_CHARS_MISSING : state.INPUT_CHARS_ERROR;
            state.inputCharsCache.set(jid, marker);
            if (state.currentJobId === jid && ui.elements.fileWordCount) {
                ui.elements.fileWordCount.textContent = (marker === state.INPUT_CHARS_MISSING) ? '-' : '读取失败';
            }
            return;
        }

        const n = Number(res.data?.input_chars);
        if (!Number.isFinite(n) || n < 0) {
            state.inputCharsCache.set(jid, state.INPUT_CHARS_ERROR);
            if (state.currentJobId === jid && ui.elements.fileWordCount) {
                ui.elements.fileWordCount.textContent = '读取失败';
            }
            return;
        }

        const count = Math.floor(n);
        state.inputCharsCache.set(jid, count);
        if (state.currentJobId === jid && ui.elements.fileWordCount) {
            ui.elements.fileWordCount.textContent = count.toLocaleString();
        }
    } catch (e) {
        state.inputCharsCache.set(jid, state.INPUT_CHARS_ERROR);
        if (state.currentJobId === jid && ui.elements.fileWordCount) {
            ui.elements.fileWordCount.textContent = '读取失败';
        }
    } finally {
        state.inputCharsInFlight.delete(jid);
    }
}

async function refreshChunksNow(jobId) {
    if (!jobId) return;
    if (state.chunksFetchInFlight) return;
    state.chunksFetchInFlight = true;
    try {
        const res = await api.fetchJobChunks(jobId, state.currentFilter);
        if (!res.ok) {
            ui.show(res.error);
            return;
        }
        state.chunksData = res.data.chunks || [];
        state.chunkCounts = res.data.chunk_counts || null;
        state.totalChunksFromServer = Number(res.data?.job?.progress?.total_chunks || 0);
        ui.updateStats();
        ui.renderChunksTable();
    } catch (e) {
        ui.show(String(e));
    } finally {
        state.chunksFetchInFlight = false;
    }
}

function stopPolling() {
    if (!state.pollTimer) return;
    clearInterval(state.pollTimer);
    state.pollTimer = null;
    state.pollJobId = null;
}

function ensurePolling(jobId) {
    if (state.pollTimer && state.pollJobId === jobId) return;
    stopPolling();
    state.pollJobId = jobId;
    state.pollTimer = setInterval(() => refreshJobOnce(jobId), 1000);
}

function detachUi({ clearFile = true } = {}) {
    stopPolling();
    state.pollInFlight = false;

    state.currentJobId = null;
    state.currentJobState = null;
    state.currentJobPhase = null;
    setAttachedJobId(null);

    state.chunksData = [];
    state.chunkCounts = null;
    state.totalChunksFromServer = 0;
    ui.updateStats();
    ui.renderChunksTable();

    ui.setJobCard(null);
    ui.setProgressFromJob(null);
    ui.refreshLocks(null);
    ui.refreshActionButtons(null);
    ui.clearShow();

    if (clearFile && ui.elements.fileInput) {
        ui.elements.fileInput.value = '';
    }
    ui.refreshSourcePanelFromJob(null);
}

async function refreshJobOnce(jobId) {
    if (!jobId || state.currentJobId !== jobId) return;
    if (state.pollInFlight) return;
    state.pollInFlight = true;
    try {
        const r = await api.fetchJobSummary(jobId);
        if (!jobId || state.currentJobId !== jobId) return;
        if (!r.ok) {
            if (r.status === 404) {
                detachUi();
                ui.show('任务已被清理或不存在。');
                return;
            }
            ui.show(r.error);
            return;
        }

        const job = r?.data?.job;
        state.currentJobState = job?.state || null;
        state.currentJobPhase = job?.phase || null;

        // Sync slice settings from job
        const fmt = job?.format || null;
        if (fmt) {
            const setCheck = (name, val) => {
                const el = ui.elements.form.querySelector(`[name="${name}"]`);
                if (el) el.checked = !!val;
            };
            const setNum = (name, val) => {
                const el = ui.elements.form.querySelector(`[name="${name}"]`);
                if (el && val != null) el.value = String(val);
            };
            setNum('max_chunk_chars', fmt.max_chunk_chars);
            setCheck('paragraph_indent', fmt.paragraph_indent);
            setCheck('indent_with_fullwidth_space', fmt.indent_with_fullwidth_space);
            setCheck('normalize_blank_lines', fmt.normalize_blank_lines);
            setCheck('trim_trailing_spaces', fmt.trim_trailing_spaces);
            setCheck('normalize_ellipsis', fmt.normalize_ellipsis);
            setCheck('normalize_em_dash', fmt.normalize_em_dash);
            setCheck('normalize_cjk_punctuation', fmt.normalize_cjk_punctuation);
            setCheck('fix_cjk_punct_spacing', fmt.fix_cjk_punct_spacing);
            setCheck('normalize_quotes', fmt.normalize_quotes);
        }

        ui.setJobCard(job);
        ui.setProgressFromJob(job);
        ui.refreshLocks(job);
        ui.refreshActionButtons(job);
        ui.refreshSourcePanelFromJob(job);

        state.chunkCounts = r?.data?.chunk_counts || null;
        state.totalChunksFromServer = Number(job?.progress?.total_chunks || 0);
        ui.updateStats();

        if (job?.state === 'error') {
            ui.show(job?.error || '处理出错');
        } else if (job?.state === 'paused' && job?.phase === 'process') {
            const done = Number(job?.progress?.done_chunks || 0);
            ui.show(done > 0 ? '校对已暂停：可点击“继续校对”。' : '预处理完成：可点击“开始校对”。');
        } else if (job?.state === 'paused' && job?.phase === 'merge') {
            ui.show('校对完成：可点击“合并输出”。');
        } else if (job?.state === 'done') {
            ui.show(job?.output_path ? `完成。输出：${job.output_path}\n可点击“下载输出”或“新任务”。` : '完成。已输出到项目 output/ 目录。');
        }

        const counts = state.chunkCounts || {};
        const active = Number(counts.processing || 0) + Number(counts.retrying || 0);
        const stLower = String(job?.state || '').toLowerCase();
        const shouldPoll = stLower === 'queued' || stLower === 'running' || (stLower === 'paused' && active > 0);
        if (shouldPoll) ensurePolling(jobId);
        else stopPolling();

        if (state.activeTab === 'debug') {
            state.forceChunksFetch = true;
            await refreshChunksNow(jobId);
        }
        
        // Ensure input chars are loaded
        const jid = String(job?.id || '').trim();
        const cached = jid ? state.inputCharsCache.get(jid) : null;
        if (jid && cached == null) ensureJobInputChars(jid);

    } finally {
        state.pollInFlight = false;
    }
}

async function attachJob(jobId) {
    const jid = String(jobId || '').trim();
    if (!jid) return;
    state.currentJobId = jid;
    setAttachedJobId(jid);

    state.chunksData = [];
    state.chunkCounts = null;
    state.totalChunksFromServer = 0;
    ui.updateStats();
    ui.renderChunksTable();

    ensurePolling(jid);
    await refreshJobOnce(jid);
}

// --- LLM Settings Logic ---

function _llmSnapshot() {
    const fd = new FormData(ui.elements.form);
    return JSON.stringify({
        base_url: String(fd.get('llm_base_url') || ''),
        model: String(fd.get('llm_model') || ''),
        api_key: String(fd.get('llm_api_key') || ''),
        temperature: String(fd.get('llm_temperature') || ''),
        timeout_seconds: String(fd.get('llm_timeout_seconds') || ''),
        max_concurrency: String(fd.get('llm_max_concurrency') || ''),
        extra_params: String(fd.get('llm_extra_params') || '').trim(),
    });
}

function _refreshLlmDirty() {
    if (state.llmSavedSnapshot == null) return;
    ui.setLlmDirty(_llmSnapshot() !== state.llmSavedSnapshot);
}

async function loadLlmDefaults() {
    try {
        const res = await api.fetchLlmSettings();
        if (!res.ok) {
            ui.show(res.error);
            return;
        }
        const llm = res.data?.llm || {};
        const setIfProvided = (name, value) => {
            if (value === undefined || value === null) return;
            const el = ui.elements.form.querySelector(`[name="${name}"]`);
            if (!el) return;
            el.value = String(value);
        };

        setIfProvided('llm_base_url', llm.base_url);
        setIfProvided('llm_model', llm.model);
        setIfProvided('llm_api_key', llm.api_key);
        if (llm.temperature !== undefined && llm.temperature !== null) setIfProvided('llm_temperature', llm.temperature);
        if (llm.timeout_seconds !== undefined && llm.timeout_seconds !== null) setIfProvided('llm_timeout_seconds', llm.timeout_seconds);
        if (llm.max_concurrency !== undefined && llm.max_concurrency !== null) setIfProvided('llm_max_concurrency', llm.max_concurrency);
        if (llm.extra_params !== undefined && llm.extra_params !== null) {
            const el = ui.elements.form.querySelector('[name="llm_extra_params"]');
            if (el) el.value = JSON.stringify(llm.extra_params, null, 2);
        }
    } catch (e) {
        ui.show(String(e));
    } finally {
        state.llmSavedSnapshot = _llmSnapshot();
        ui.setLlmDirty(false);
    }
}

function parseExtraParamsFromFormData(fd) {
    const raw = String(fd.get('llm_extra_params') || '').trim();
    if (!raw) return { ok: true, value: null };
    try {
        const obj = JSON.parse(raw);
        if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
            return { ok: false, error: '额外参数必须是 JSON 对象（例如 {"max_tokens": 4096}）。' };
        }
        return { ok: true, value: obj };
    } catch (e) {
        return { ok: false, error: '额外参数 JSON 无效，请检查括号/引号/逗号。' };
    }
}

function buildLlmOptions(fd, extraParamsValue) {
    return {
        base_url: String(fd.get('llm_base_url') || ''),
        api_key: String(fd.get('llm_api_key') || ''),
        model: String(fd.get('llm_model') || ''),
        temperature: Number(fd.get('llm_temperature') || 0),
        timeout_seconds: Number(fd.get('llm_timeout_seconds') || 180),
        max_concurrency: Number(fd.get('llm_max_concurrency') || 20),
        extra_params: extraParamsValue,
    };
}

function buildJobOptions(fd, extraParamsValue) {
    const cleanupDebugDir = !!document.querySelector('input[type="checkbox"][name="cleanup_debug_dir"]')?.checked;
    return {
        format: {
            max_chunk_chars: Number(fd.get('max_chunk_chars') || 2000),
            paragraph_indent: fd.get('paragraph_indent') != null,
            indent_with_fullwidth_space: fd.get('indent_with_fullwidth_space') != null,
            normalize_blank_lines: fd.get('normalize_blank_lines') != null,
            trim_trailing_spaces: fd.get('trim_trailing_spaces') != null,
            normalize_ellipsis: fd.get('normalize_ellipsis') != null,
            normalize_em_dash: fd.get('normalize_em_dash') != null,
            normalize_cjk_punctuation: fd.get('normalize_cjk_punctuation') != null,
            fix_cjk_punct_spacing: fd.get('fix_cjk_punct_spacing') != null,
            normalize_quotes: fd.get('normalize_quotes') != null,
        },
        llm: buildLlmOptions(fd, extraParamsValue),
        output: {
            suffix: String(fd.get('suffix') || '_rev'),
            cleanup_debug_dir: cleanupDebugDir,
        },
    };
}

// --- Initialization ---

document.addEventListener('DOMContentLoaded', () => {
    ui.initElements();
    modal.initModal();

    // 1. Load State
    loadUiState(ui.elements.form);
    ui.refreshFileName();
    ui.refreshActionButtons(null);
    
    // 2. Setup Events
    UI_STATE_FIELDS.forEach((name) => {
        let el;
        if (name === 'cleanup_debug_dir') {
             el = ui.elements.form.querySelector('input[type="checkbox"][name="cleanup_debug_dir"]');
        } else {
             el = ui.elements.form.querySelector(`[name="${name}"]`);
        }
        if (!el) return;
        el.addEventListener('input', () => {
            saveUiState(ui.elements.form);
            if (name === 'suffix') ui.refreshOutputPreview();
        });
        el.addEventListener('change', () => {
            saveUiState(ui.elements.form);
            if (name === 'suffix') ui.refreshOutputPreview();
        });
    });

    ui.elements.fileInput?.addEventListener('change', () => {
        ui.refreshFileName();
        if (!state.currentJobId) ui.refreshActionButtons(null);
    });
    
    // Drag & Drop
    if (ui.elements.fileInput && ui.elements.fileDrop) {
        const addDrag = () => {
            ui.elements.fileDrop.classList.add('border-ink', 'bg-slate-50');
        };
        const rmDrag = () => {
            ui.elements.fileDrop.classList.remove('border-ink', 'bg-slate-50');
        };
        ui.elements.fileInput.addEventListener('dragenter', addDrag);
        ui.elements.fileInput.addEventListener('dragleave', rmDrag);
        ui.elements.fileInput.addEventListener('drop', () => {
            rmDrag();
            setTimeout(() => {
                ui.refreshFileName();
                if (!state.currentJobId) ui.refreshActionButtons(null);
            }, 0);
        });
    }

    // Tabs
    ui.elements.tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            ui.switchTab(btn.dataset.tab, () => {
                saveActiveTab(btn.dataset.tab);
                if (btn.dataset.tab === 'debug' && state.currentJobId) {
                    state.forceChunksFetch = true;
                    refreshChunksNow(state.currentJobId);
                }
            });
        });
    });
    
    ui.switchTab(getSavedActiveTab());

    // Filter Buttons
    ui.elements.filterBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            state.currentFilter = btn.dataset.filter;
            ui.elements.filterBtns.forEach(b => b.classList.toggle('active', b === btn));
            if (state.activeTab === 'debug' && state.currentJobId) {
                state.forceChunksFetch = true;
                refreshChunksNow(state.currentJobId);
            } else {
                ui.renderChunksTable();
            }
        });
    });

    // LLM Save
    ui.elements.btnSaveLlmDefaults.addEventListener('click', async () => {
        ui.elements.btnSaveLlmDefaults.disabled = true;
        const fd = new FormData(ui.elements.form);
        const extra = parseExtraParamsFromFormData(fd);
        if (!extra.ok) return { ok: false, error: extra.error };
        
        const payload = {
            llm: {
                base_url: String(fd.get('llm_base_url') || ''),
                api_key: String(fd.get('llm_api_key') || ''),
                model: String(fd.get('llm_model') || ''),
                temperature: Number(fd.get('llm_temperature') || 0),
                timeout_seconds: Number(fd.get('llm_timeout_seconds') || 180),
                max_concurrency: Number(fd.get('llm_max_concurrency') || 20),
                extra_params: extra.value,
            }
        };

        const r = await api.saveLlmSettings(payload);
        if (!r.ok) {
            ui.show(r.error);
            _refreshLlmDirty();
            return;
        }
        ui.show('默认 LLM 配置已保存。');
        state.llmSavedSnapshot = _llmSnapshot();
        ui.setLlmDirty(false);
    });

    // LLM Dirty Check
    ['llm_base_url', 'llm_model', 'llm_api_key', 'llm_temperature', 'llm_timeout_seconds', 'llm_max_concurrency', 'llm_extra_params'].forEach((name) => {
        const el = ui.elements.form.querySelector(`[name="${name}"]`);
        if (!el) return;
        el.addEventListener('input', _refreshLlmDirty);
        el.addEventListener('change', _refreshLlmDirty);
    });

    ui.elements.btnToggleApiKey.addEventListener('click', ui.toggleApiKeyVisibility);

    // Job Actions
    ui.elements.btnPause?.addEventListener('click', async () => {
        const jobId = state.currentJobId;
        if (!jobId) return;
        ui.elements.btnPause.disabled = true;
        ui.elements.btnPause.classList.add('opacity-50', 'cursor-not-allowed');
        ui.show('正在暂停…');
        const r = await api.pauseJob(jobId);
        if (!r.ok) {
            ui.show(r.error);
            ui.elements.btnPause.disabled = false;
            ui.elements.btnPause.classList.remove('opacity-50', 'cursor-not-allowed');
            return;
        }
        await refreshJobOnce(jobId);
    });

    ui.elements.btnRetry?.addEventListener('click', async () => {
        const jobId = state.currentJobId;
        if (!jobId) return;
        ui.show('准备重试失败分片…');
        
        const fd = new FormData(ui.elements.form);
        const extra = parseExtraParamsFromFormData(fd);
        if (!extra.ok) { ui.show(extra.error); return; }
        const payload = { llm: buildLlmOptions(fd, extra.value) };

        const r = await api.retryFailedJob(jobId, payload);
        if (!r.ok) { ui.show(r.error); return; }
        await refreshJobOnce(jobId);
    });

    ui.elements.btnProcess?.addEventListener('click', async () => {
        const jobId = state.currentJobId;
        if (!jobId) return;
        ui.show('开始校对…');

        const fd = new FormData(ui.elements.form);
        const extra = parseExtraParamsFromFormData(fd);
        if (!extra.ok) { ui.show(extra.error); return; }
        const payload = { llm: buildLlmOptions(fd, extra.value) };

        const r = await api.resumeJob(jobId, payload);
        if (!r.ok) { ui.show(r.error); return; }
        await refreshJobOnce(jobId);
    });

    ui.elements.btnMerge?.addEventListener('click', async () => {
        const jobId = state.currentJobId;
        if (!jobId) return;
        ui.show('开始合并输出…');
        
        const cleanupDebugDir = !!document.querySelector('input[type="checkbox"][name="cleanup_debug_dir"]')?.checked;
        const payload = { cleanup_debug_dir: cleanupDebugDir };

        const r = await api.mergeJob(jobId, payload);
        if (!r.ok) { ui.show(r.error); return; }
        await refreshJobOnce(jobId);
    });

    ui.elements.btnDownload?.addEventListener('click', () => {
        api.downloadJobOutput(state.currentJobId);
    });

    ui.elements.btnValidate?.addEventListener('click', async () => {
        if (!state.currentJobId) {
            ui.elements.form.requestSubmit();
            return;
        }
        if (state.currentJobState === 'paused' && state.currentJobPhase === 'validate') {
            const jobId = state.currentJobId;
            ui.show('继续预处理…');
            
            const fd = new FormData(ui.elements.form);
            const extra = parseExtraParamsFromFormData(fd);
            if (!extra.ok) { ui.show(extra.error); return; }
            const payload = { llm: buildLlmOptions(fd, extra.value) };

            const r = await api.resumeJob(jobId, payload);
            if (!r.ok) { ui.show(r.error); return; }
            await refreshJobOnce(jobId);
        }
    });

    ui.elements.btnCancel?.addEventListener('click', async () => {
        const jobId = state.currentJobId;
        if (!jobId) return;
        const st = String(state.currentJobState || '').toLowerCase();
        const phase = String(state.currentJobPhase || '').toLowerCase();
        const isRunning = st === 'queued' || st === 'running';
        if (isRunning) {
            if (phase === 'process') ui.show('校对进行中：请先“暂停”再开始新任务。');
            else ui.show('任务运行中：请等待完成后再开始新任务，或直接删除任务。');
            return;
        }
        detachUi({ clearFile: true });
        ui.show('已切换到新任务。可通过“加载任务”返回该任务继续/下载。');
    });

    ui.elements.btnReset?.addEventListener('click', async () => {
        const jobId = state.currentJobId;
        if (!jobId) return;
        const st = String(state.currentJobState || '').toLowerCase();
        const phase = String(state.currentJobPhase || '').toLowerCase();
        
        if ((st === 'queued' || st === 'running') && phase === 'process') {
            ui.show('校对进行中：请先“暂停”再删除任务。');
            return;
        }
        
        const isRunning = st === 'queued' || st === 'running';
        const isDone = st === 'done';
        const isValidateRunning = isRunning && phase === 'validate';
        const isMergeRunning = isRunning && phase === 'merge';
        const phaseLabel = isValidateRunning ? '预处理' : (isMergeRunning ? '合并' : '');
        
        const confirmMsg = isDone
            ? '确认"删除任务记录"？该任务的中间态/状态记录将被删除且不可恢复（不会删除最终输出文件 output/）。'
            : (isValidateRunning || isMergeRunning)
                ? `确认"删除任务"？任务正在${phaseLabel}中：将尝试停止当前阶段，完成停止后删除该任务的中间产物与状态记录（不可恢复；不会删除 output/ 下已生成的最终输出文件）。`
                : '确认"删除任务"？该任务将被删除：中间产物与状态记录将被清理且不可恢复（不会删除 output/ 下已生成的最终输出文件）。';

        if (!(await modal.showConfirm(confirmMsg))) return;

        ui.show((st === 'done') ? '正在删除任务记录…' : '正在删除任务…');
        const r = await api.resetJob(jobId);
        if (!r.ok) { ui.show(r.error); return; }
        
        detachUi({ clearFile: true });
        ui.show((isValidateRunning || isMergeRunning) ? '已提交删除请求：后台停止/清理中。' : '已提交删除请求。');
    });

    ui.elements.btnLoad?.addEventListener('click', async () => {
        const r = await api.fetchJobList();
        if (!r.ok) { ui.show(r.error); return; }
        const jobs = Array.isArray(r?.data?.jobs) ? r.data.jobs : [];
        if (!jobs.length) { ui.show('暂无可加载任务。'); return; }

        const chosenId = await modal.showJobPicker(jobs);
        if (!chosenId) return;
        await attachJob(chosenId);
    });

    // Form Submit (New Job)
    ui.elements.form.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (state.currentJobId) return;
        if (state.createJobInFlight) return;
        detachUi({ clearFile: false });

        const fd = new FormData(ui.elements.form);
        const extra = parseExtraParamsFromFormData(fd);
        if (!extra.ok) { ui.show(extra.error); return; }
        
        const maxChunkChars = Number(fd.get('max_chunk_chars') || 0);
        if (!Number.isFinite(maxChunkChars) || maxChunkChars < 200 || maxChunkChars > 4000) {
            ui.show('分片大小（字符数）必须在 200-4000 之间。');
            return;
        }
        ui.show('已提交，准备预处理…');

        const file = fd.get('file');
        if (!(file instanceof File)) {
            ui.show('请选择 TXT 文件。');
            return;
        }

        state.createJobInFlight = true;
        ui.refreshActionButtons(null);

        try {
            const opts = buildJobOptions(fd, extra.value);
            const requestFd = new FormData();
            requestFd.append('file', file);
            requestFd.append('options', JSON.stringify(opts));

            const res = await api.createJob(requestFd);
            if (!res.ok) {
                ui.show(res.error || '创建任务失败。');
                return;
            }

            const jobId = String(res.data?.job?.id || '').trim();
            if (!jobId) {
                ui.show('创建任务失败：返回缺少 job_id。');
                return;
            }
            await attachJob(jobId);
        } catch (e) {
            ui.show('创建任务失败：网络或服务异常。');
        } finally {
            state.createJobInFlight = false;
            if (!state.currentJobId) ui.refreshActionButtons(null);
        }
    });

    // Page Lifecycle
    window.addEventListener('pagehide', () => api.bestEffortPauseJob(state.currentJobId));
    window.addEventListener('beforeunload', () => api.bestEffortPauseJob(state.currentJobId));
    
    // Auto Reattach
    const last = getAttachedJobId();
    if (last) attachJob(last);

    loadLlmDefaults();
});
