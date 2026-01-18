
import { state, UI_STATE_FIELDS } from './state.js';

// DOM Elements Cache
export const elements = {};

export function initElements() {
    const id = (i) => document.getElementById(i);
    const qs = (s) => document.querySelector(s);
    
    elements.form = id('mainForm');
    elements.out = id('out');
    elements.outText = id('outText');
    elements.progress = id('progress');
    elements.bar = id('bar');
    elements.metaLeft = id('metaLeft');
    elements.metaCounter = id('metaCounter');
    elements.metaPctWrap = id('metaPctWrap');
    elements.metaPct = id('metaPct');
    elements.metaModelWrap = id('metaModelWrap');
    elements.metaModel = id('metaModel');
    elements.wfStateBadge = id('wfStateBadge');
    
    elements.btnValidate = id('btnValidate');
    elements.btnProcess = id('btnProcess');
    elements.btnMerge = id('btnMerge');
    elements.btnPause = id('btnPause');
    elements.btnRetry = id('btnRetry');
    elements.btnCancel = id('btnCancel');
    elements.btnReset = id('btnReset');
    elements.btnDownload = id('btnDownload');
    elements.btnLoad = id('btnLoad');
    elements.btnSaveLlmDefaults = id('btnSaveLlmDefaults');
    elements.btnToggleApiKey = id('btnToggleApiKey');
    
    elements.llmDirtyHint = id('llmDirtyHint');
    elements.fileInput = elements.form.querySelector('input[name="file"]');
    elements.suffixInput = elements.form.querySelector('input[name="suffix"]');
    elements.outputPreview = id('outputPreview');
    
    elements.fileDrop = id('fileDrop');
    elements.fileDropText = id('fileDropText');
    elements.fileName = id('fileName');
    elements.fileInfoPanel = id('fileInfoPanel');
    elements.fileInfoEmpty = id('fileInfoEmpty');
    elements.fileInfoEmptyText = id('fileInfoEmptyText');
    elements.fileInfoContent = id('fileInfoContent');
    elements.fileWordCount = id('fileWordCount');
    
    elements.jobId = id('jobId');
    elements.jobInputName = id('jobInputName');
    elements.sliceLockHint = id('sliceLockHint');
    elements.llmLockHint = id('llmLockHint');
    
    elements.chunksBody = id('chunksBody');
    elements.noChunksHint = id('noChunksHint');
    elements.chunksTableWrap = id('chunksTableWrap');
    
    elements.statTotal = id('statTotal');
    elements.statDone = id('statDone');
    elements.statProcessing = id('statProcessing');
    elements.statPending = id('statPending');
    elements.statError = id('statError');
    
    elements.tabBtns = document.querySelectorAll('.tab-btn');
    elements.tabContents = document.querySelectorAll('.tab-content');
    elements.filterBtns = document.querySelectorAll('.filter-btn');
    elements.wfSteps = document.querySelectorAll('[data-wf-step]');
}

// Helpers
function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function truncateStr(s, maxLen) {
    const str = String(s || '');
    const n = Number(maxLen || 0);
    if (!Number.isFinite(n) || n <= 0) return str;
    if (str.length <= n) return str;
    return str.slice(0, n) + '…';
}

function tryParseJson(s) {
    try { return JSON.parse(s); } catch (e) { return null; }
}

export function show(obj) {
    if (!elements.out) return;
    elements.out.classList.remove('hidden');
    elements.outText.textContent = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
}

export function clearShow() {
    if (!elements.out) return;
    elements.out.classList.add('hidden');
    elements.outText.textContent = '';
}

export function refreshOutputPreview() {
    if (!elements.outputPreview) return;
    const file = elements.fileInput?.files?.[0];
    const rawName = String(file?.name || 'input.txt');
    const name = rawName.split(/[\\/]/).pop() || 'input.txt';
    const dot = name.lastIndexOf('.');
    const stem = (dot > 0) ? name.slice(0, dot) : name;
    const ext = (dot > 0) ? name.slice(dot) : '.txt';
    const suffix = String(elements.suffixInput?.value || '_rev').trim() || '_rev';
    elements.outputPreview.textContent = `output/<job_id>_${stem}${suffix}${ext}`;
}

export function refreshFileName(inputCharsCallback) { // inputCharsCallback is optional async function to get word count
    const file = elements.fileInput?.files?.[0];
    if (file) {
        if (elements.fileName) elements.fileName.textContent = file.name;
        if (elements.fileInfoEmpty) elements.fileInfoEmpty.classList.add('hidden');
        if (elements.fileInfoContent) elements.fileInfoContent.classList.remove('hidden');
        if (elements.fileWordCount) elements.fileWordCount.textContent = '读取中...';
        
        file.text().then(text => {
            const count = String(text || '').replace(/\s/g, '').length;
            if (elements.fileWordCount) elements.fileWordCount.textContent = count.toLocaleString();
        }).catch(() => {
            if (elements.fileWordCount) elements.fileWordCount.textContent = '读取失败';
        });
    } else {
        if (elements.fileName) elements.fileName.textContent = '-';
        if (elements.fileInfoEmpty) elements.fileInfoEmpty.classList.remove('hidden');
        if (elements.fileInfoContent) elements.fileInfoContent.classList.add('hidden');
        if (elements.fileWordCount) elements.fileWordCount.textContent = '-';
    }
    refreshOutputPreview();
}

export function refreshSourcePanelFromJob(job) {
    const hasJob = !!job?.id;
    if (elements.fileInput) elements.fileInput.disabled = hasJob;
    if (elements.fileDrop) {
        elements.fileDrop.classList.toggle('cursor-pointer', !hasJob);
        elements.fileDrop.classList.toggle('cursor-not-allowed', hasJob);
        elements.fileDrop.classList.toggle('opacity-60', hasJob);
        elements.fileDrop.classList.toggle('hover:border-ink/50', !hasJob);
        elements.fileDrop.classList.toggle('hover:bg-slate-50', !hasJob);
    }

    if (hasJob) {
        const jid = String(job?.id || '').trim();
        const cached = jid ? state.inputCharsCache.get(jid) : null;
        
        if (elements.fileDropText) elements.fileDropText.textContent = '已加载任务（无需重新选择文件）';
        if (elements.fileInfoEmptyText) elements.fileInfoEmptyText.textContent = '已加载任务（无需重新选择文件）';
        if (elements.fileName) elements.fileName.textContent = String(job?.input_filename || '-');
        
        if (elements.fileWordCount) {
            if (typeof cached === 'number') elements.fileWordCount.textContent = cached.toLocaleString();
            else if (cached === state.INPUT_CHARS_MISSING) elements.fileWordCount.textContent = '-';
            else if (cached === state.INPUT_CHARS_ERROR) elements.fileWordCount.textContent = '读取失败';
            else elements.fileWordCount.textContent = '读取中...';
        }
        
        if (elements.fileInfoEmpty) elements.fileInfoEmpty.classList.add('hidden');
        if (elements.fileInfoContent) elements.fileInfoContent.classList.remove('hidden');
        return;
    }

    if (elements.fileDropText) elements.fileDropText.textContent = '点击选择或拖拽 TXT 文件';
    if (elements.fileInfoEmptyText) elements.fileInfoEmptyText.textContent = '未选择任何文件';
    refreshFileName(); // Reset to local file state
}

export function updateStats() {
    const counts = state.chunkCounts || {};
    elements.statTotal.textContent = String(state.totalChunksFromServer || 0);
    elements.statDone.textContent = String(counts.done || 0);
    elements.statProcessing.textContent = String((counts.processing || 0) + (counts.retrying || 0));
    elements.statPending.textContent = String(counts.pending || 0);
    elements.statError.textContent = String(counts.error || 0);
}

function getStatusBadge(stateName) {
    const config = {
        pending: { class: 'bg-slate-100 text-slate-500', label: '待处理' },
        processing: { class: 'bg-blue-50 text-blue-600', label: '处理中' },
        done: { class: 'bg-green-50 text-green-600', label: '完成' },
        error: { class: 'bg-red-50 text-red-600', label: '错误' },
        retrying: { class: 'bg-amber-50 text-amber-600', label: '处理中' }
    };
    const c = config[stateName] || config.pending;
    return `<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${c.class}">${c.label}</span>`;
}

function formatErrorDisplay(code, message) {
    const codeStr = (code == null || code === '') ? '' : String(code);
    const msg = (message == null) ? '' : String(message).trim();

    if (!codeStr && !msg) return null;

    const httpMatch = msg.match(/^HTTP\s+(\d+)\s+from\s+LLM:\s*(.*)$/i);
    if (httpMatch) {
        const httpCode = httpMatch[1] || codeStr || '';
        const body = String(httpMatch[2] || '').trim();
        let reason = '';
        if (body) {
            const parsed = (body.startsWith('{') || body.startsWith('[')) ? tryParseJson(body) : null;
            if (parsed && parsed.error && parsed.error.message) reason = String(parsed.error.message);
            else if (parsed && parsed.message) reason = String(parsed.message);
            else reason = body;
        }
        const brief = httpCode ? `HTTP ${httpCode}` : 'HTTP error';
        const detail = reason ? `${brief}: ${reason}` : brief;
        return { brief: truncateStr(brief, 40), detail: truncateStr(detail, 300) };
    }
    
    // ... other error patterns from original code ...
    if (/LLM output empty/i.test(msg)) {
        return { brief: 'LLM output empty', detail: 'LLM output empty' };
    }
    if (/LLM output too short/i.test(msg)) {
        const m = msg.match(/ratio=([0-9.]+)/i);
        const brief = m ? `LLM output too short (ratio=${m[1]})` : 'LLM output too short';
        return { brief: truncateStr(brief, 60), detail: truncateStr(msg || brief, 300) };
    }
    if (/LLM output too long/i.test(msg)) {
        const m = msg.match(/ratio=([0-9.]+)/i);
        const brief = m ? `LLM output too long (ratio=${m[1]})` : 'LLM output too long';
        return { brief: truncateStr(brief, 60), detail: truncateStr(msg || brief, 300) };
    }
    if (codeStr) {
        const brief = `HTTP ${codeStr}`;
        const detail = msg ? `${brief}: ${msg}` : brief;
        return { brief: truncateStr(brief, 40), detail: truncateStr(detail, 300) };
    }
    const brief = truncateStr(msg, 60);
    return { brief, detail: truncateStr(msg, 300) };
}

export function renderChunksTable() {
    if (!state.chunksData || state.chunksData.length === 0) {
        elements.noChunksHint.classList.remove('hidden');
        elements.chunksTableWrap.classList.add('hidden');
        if (state.currentJobId) {
            if (state.currentFilter === 'all') {
                elements.noChunksHint.textContent = '暂无分片数据（可切换到调试信息页以加载）。';
            } else {
                elements.noChunksHint.textContent = '当前过滤条件下无匹配分片。';
            }
        } else {
            elements.noChunksHint.textContent = '暂无分片数据，请先开始校对任务。';
        }
        return;
    }

    elements.noChunksHint.classList.add('hidden');
    elements.chunksTableWrap.classList.remove('hidden');

    const rows = state.chunksData.slice(0, 500).map(c => {
        let inOut = '-';
        if (c.input_chars != null && c.output_chars != null) inOut = `${c.input_chars} / ${c.output_chars}`;
        else if (c.input_chars != null) inOut = `${c.input_chars} / -`;

        const llmFull = (c.llm_model != null) ? String(c.llm_model) : '';
        const llmCell = llmFull
            ? `<div class="truncate max-w-[9rem] mx-auto text-slate-600 font-medium" title="${escapeHtml(llmFull)}">${escapeHtml(llmFull)}</div>`
            : `<span class="text-slate-300">-</span>`;

        const errCode = (c.last_error_code != null) ? String(c.last_error_code) : '';
        const errFull = c.last_error_message ? String(c.last_error_message) : '';
        const err = formatErrorDisplay(errCode, errFull);
        let errMsg = '-';
        if (err) {
            const detail = escapeHtml(err.detail);
            const brief = escapeHtml(err.brief);
            if (c.state === 'error') {
                errMsg = `<span class="text-red-600 font-medium cursor-help border-b border-dotted border-red-300 hover:text-red-700" title="${detail}">${brief}</span>`;
            } else if (c.state === 'retrying') {
                errMsg = `<span class="text-amber-600 font-medium cursor-help border-b border-dotted border-amber-300 hover:text-amber-700" title="${detail}">${brief}</span>`;
            } else if (c.state === 'done') {
                const retries = Number(c.retries || 0);
                if (retries > 0) {
                    errMsg = `<span class="text-slate-400 cursor-help border-b border-dotted border-slate-300 hover:text-slate-600" title="${detail}">曾重试: ${brief}</span>`;
                }
            } else {
                errMsg = `<span class="text-slate-400 cursor-help border-b border-dotted border-slate-300" title="${detail}">${brief}</span>`;
            }
        }

        return `<tr class="hover:bg-slate-50 transition-colors border-b border-slate-50 last:border-0">
           <td class="py-3 px-2 text-slate-400 font-mono text-center whitespace-nowrap text-[11px]">${c.index}</td>
           <td class="py-3 px-2 text-center whitespace-nowrap">${getStatusBadge(c.state)}</td>
           <td class="py-3 px-2 whitespace-nowrap text-xs text-center">${llmCell}</td>
           <td class="py-3 px-2 text-slate-400 text-center whitespace-nowrap font-mono text-xs">${c.retries || 0}</td>
           <td class="py-3 px-2 text-slate-400 font-mono text-[11px] text-center whitespace-nowrap tracking-tight">${inOut}</td>
           <td class="py-3 px-2 text-slate-600 text-xs text-center">${errMsg}</td>
         </tr>`;
    }).join('');

    elements.chunksBody.innerHTML = rows;
}

export function setJobCard(job) {
    const j = job || null;
    if (elements.jobId) elements.jobId.textContent = j?.id ? String(j.id) : '-';
    if (elements.jobInputName) elements.jobInputName.textContent = j?.input_filename ? String(j.input_filename) : '-';
}

function _stateLabel(s) {
    const v = String(s || '').toLowerCase();
    const map = { queued: '排队中', running: '运行中', paused: '已暂停', done: '已完成', cancelled: '已删除', error: '出错' };
    return map[v] || (v || '-');
}

export function setProgressFromJob(job) {
    if (!job) {
        if (elements.progress) elements.progress.classList.add('hidden');
        if (elements.metaModelWrap) elements.metaModelWrap.classList.add('hidden');
        return;
    }

    if (elements.progress) elements.progress.classList.remove('hidden');
    const st = String(job.state || '').toLowerCase();
    const phase = String(job.phase || '').toLowerCase();
    const done = Number(job?.progress?.done_chunks || 0);
    const total = Number(job?.progress?.total_chunks || 0);
    const pct = total > 0 ? Math.floor((done / total) * 100) : (st === 'done' ? 100 : 0);
    
    if (elements.bar) {
        elements.bar.style.width = pct + '%';
        elements.bar.classList.remove('bg-ink', 'bg-amber-500', 'bg-red-500', 'bg-emerald-500');
        if (st === 'error') elements.bar.classList.add('bg-red-500');
        else if (st === 'paused') elements.bar.classList.add('bg-amber-500');
        else if (st === 'done') elements.bar.classList.add('bg-emerald-500');
        else elements.bar.classList.add('bg-ink');
    }

    let left = _stateLabel(st);
    if (st === 'running' && phase === 'validate') left = '正在预处理…';
    if (st === 'paused' && phase === 'process') left = (done > 0 ? '校对已暂停' : '预处理完成，等待开始校对');
    if (st === 'running' && phase === 'process') left = '正在校对…';
    if (st === 'error' && phase === 'process') left = '校对出错';
    if (st === 'paused' && phase === 'merge') left = '校对完成，等待合并输出';
    if (st === 'running' && phase === 'merge') left = '正在合并输出…';
    if (st === 'done') left = '完成';

    if (elements.metaLeft) elements.metaLeft.textContent = left;
    if (elements.metaCounter) elements.metaCounter.textContent = `${done}/${total}`;

    const showPct = pct > 0 && pct < 100;
    if (elements.metaPctWrap) elements.metaPctWrap.classList.toggle('hidden', !showPct);
    if (elements.metaPct) elements.metaPct.textContent = String(pct);

    const model = job?.llm_model ? String(job.llm_model).trim() : '';
    if (elements.metaModel) elements.metaModel.textContent = model || '-';
    if (elements.metaModelWrap) elements.metaModelWrap.classList.toggle('hidden', !model);
}

function _setFieldsDisabled(names, disabled) {
    for (const name of names) {
        const el = elements.form.querySelector(`[name="${name}"]`);
        if (!el) continue;
        el.disabled = !!disabled;
        el.classList.toggle('opacity-50', !!disabled);
        el.classList.toggle('cursor-not-allowed', !!disabled);
    }
}

const SLICE_FIELDS = [
    'suffix', 'max_chunk_chars', 'paragraph_indent', 'indent_with_fullwidth_space',
    'normalize_blank_lines', 'trim_trailing_spaces', 'normalize_ellipsis', 'normalize_em_dash',
    'normalize_cjk_punctuation', 'fix_cjk_punct_spacing', 'normalize_quotes'
];

const LLM_FIELDS = [
    'llm_base_url', 'llm_model', 'llm_api_key', 'llm_temperature', 'llm_timeout_seconds', 'llm_max_concurrency', 'llm_extra_params'
];

export function refreshLocks(job) {
    const hasJob = !!job;
    const llmLocked = hasJob && ['queued', 'running'].includes(String(job.state || ''));

    _setFieldsDisabled(SLICE_FIELDS, hasJob);
    if (elements.sliceLockHint) {
        elements.sliceLockHint.classList.toggle('hidden', !hasJob);
        if (hasJob) elements.sliceLockHint.textContent = '当前已关联任务，切片设置已锁定（要修改请先点“新任务”解除关联，或删除任务后重新创建）。';
    }

    _setFieldsDisabled(LLM_FIELDS, llmLocked);
    if (elements.llmLockHint) {
        elements.llmLockHint.classList.toggle('hidden', !llmLocked);
        if (llmLocked) elements.llmLockHint.textContent = '任务运行中，LLM 配置已锁定（暂停/出错后可修改并用于继续/重试）。';
    }
}

export function refreshWorkflowStepper(job) {
    const st = String(job?.state || '').toLowerCase();
    const phase = String(job?.phase || '').toLowerCase();
    const hasJob = !!job;

    const order = ['validate', 'process', 'merge', 'done'];
    const labels = { validate: '预处理', process: 'LLM校对', merge: '合并', done: '完成' };
    let cur = hasJob ? phase : 'validate';
    if (!order.includes(cur)) cur = 'validate';
    if (st === 'done') cur = 'done';
    const curIdx = order.indexOf(cur);

    elements.wfSteps.forEach((el) => {
        const step = String(el.getAttribute('data-wf-step') || '').toLowerCase();
        const idx = order.indexOf(step);
        const isDone = idx >= 0 && idx < curIdx;
        const isActive = idx === curIdx;
        const isError = isActive && hasJob && st === 'error';

        el.className = 'wf-step relative pb-0.5 cursor-help transition-colors';
        if (isDone) {
            el.classList.add('text-slate-400');
            el.innerHTML = labels[step] + ' <span class="text-emerald-500">✓</span>';
            el.style.removeProperty('--wf-underline');
        } else if (isActive) {
            el.classList.add(isError ? 'text-red-600' : 'text-ink', 'font-medium');
            el.innerHTML = labels[step];
            el.style.setProperty('--wf-underline', '1');
        } else {
            el.classList.add('text-slate-400');
            el.innerHTML = labels[step];
            el.style.removeProperty('--wf-underline');
        }
    });

    if (elements.wfStateBadge) {
        let badgeText = '未开始';
        let badgeCls = 'text-xs text-slate-400';
        if (hasJob) {
            badgeText = _stateLabel(st);
            if (st === 'paused') badgeCls = 'text-xs text-amber-600';
            else if (st === 'error') badgeCls = 'text-xs text-red-600';
            else if (st === 'done') badgeCls = 'text-xs text-emerald-600';
            else badgeCls = 'text-xs text-slate-500';
        }
        elements.wfStateBadge.textContent = badgeText;
        elements.wfStateBadge.className = badgeCls;
    }
}

function _setDisabled(el, disabled) {
    if (!el) return;
    el.disabled = !!disabled;
    el.classList.toggle('opacity-50', !!disabled);
    el.classList.toggle('cursor-not-allowed', !!disabled);
}

export function refreshActionButtons(job) {
    const st = String(job?.state || '').toLowerCase();
    const phase = String(job?.phase || '').toLowerCase();
    const hasJob = !!job;

    // Styles
    const BTN_SOLID_INK = 'w-full px-5 py-2.5 bg-ink hover:bg-zinc-800 text-white text-sm font-medium rounded-lg transition-colors active:scale-[0.99]';
    const BTN_OUTLINE = 'w-full px-5 py-2.5 bg-white border border-slate-200 text-slate-700 hover:bg-slate-50 hover:border-slate-300 text-sm font-medium rounded-lg transition-colors';
    const BTN_SOLID_EMERALD = 'w-full px-5 py-2.5 bg-emerald-600 hover:bg-emerald-700 text-white text-sm font-medium rounded-lg transition-colors active:scale-[0.99]';
    const BTN_OUTLINE_EMERALD = 'w-full px-5 py-2.5 bg-white border border-emerald-200 text-emerald-700 hover:bg-emerald-50 text-sm font-medium rounded-lg transition-colors';
    const BTN_SOLID_AMBER = 'w-full px-5 py-2.5 bg-amber-500 hover:bg-amber-600 text-white text-sm font-medium rounded-lg transition-colors active:scale-[0.99]';
    const BTN_OUTLINE_AMBER = 'w-full px-5 py-2.5 bg-white border border-amber-200 text-amber-700 hover:bg-amber-50 text-sm font-medium rounded-lg transition-colors';
    const BTN_OUTLINE_RED = 'w-full px-5 py-2.5 bg-white border border-red-200 text-red-700 hover:bg-red-50 text-sm font-medium rounded-lg transition-colors';

    const canResumeValidate = hasJob && st === 'paused' && phase === 'validate';
    const hasLocalFile = !!elements.fileInput?.files?.[0];
    const canStartValidate = (!hasJob) && hasLocalFile && !state.createJobInFlight;
    const canValidate = canResumeValidate || canStartValidate;
    const isSubmitting = (!hasJob) && state.createJobInFlight;

    if (elements.btnValidate) elements.btnValidate.textContent = canResumeValidate ? '继续预处理' : (isSubmitting ? '提交中...' : '开始预处理');

    const canProcess = hasJob && st === 'paused' && phase === 'process';
    if (elements.btnProcess) {
        const done = Number(job?.progress?.done_chunks || 0);
        elements.btnProcess.textContent = (hasJob && phase === 'process' && done > 0) ? '继续校对' : '开始校对';
    }

    const canMerge = hasJob && st === 'paused' && phase === 'merge';
    const canPause = hasJob && (st === 'queued' || st === 'running') && phase === 'process';
    const canRetry = hasJob && st === 'error';
    const canNewTask = hasJob && !(st === 'queued' || st === 'running');
    const canDeleteTask = hasJob && !((st === 'queued' || st === 'running') && phase === 'process');
    const canDownload = hasJob && st === 'done';

    if (elements.btnCancel) elements.btnCancel.textContent = '新任务';
    if (elements.btnReset) elements.btnReset.textContent = (hasJob && st === 'done') ? '删除任务记录' : '删除任务';

    let primaryKey = null;
    if (!hasJob || (st === 'paused' && phase === 'validate')) primaryKey = 'validate';
    else if (st === 'paused' && phase === 'process') primaryKey = 'process';
    else if (st === 'paused' && phase === 'merge') primaryKey = 'merge';
    else if (st === 'done') primaryKey = 'download';
    else if ((st === 'queued' || st === 'running') && phase === 'process') primaryKey = 'pause';
    else if (st === 'error') primaryKey = 'retry';

    if (elements.btnValidate) elements.btnValidate.className = (primaryKey === 'validate') ? BTN_SOLID_INK : BTN_OUTLINE;
    if (elements.btnProcess) elements.btnProcess.className = (primaryKey === 'process') ? BTN_SOLID_INK : BTN_OUTLINE;
    if (elements.btnMerge) elements.btnMerge.className = (primaryKey === 'merge') ? BTN_SOLID_INK : BTN_OUTLINE;
    if (elements.btnDownload) elements.btnDownload.className = (primaryKey === 'download') ? BTN_SOLID_EMERALD : BTN_OUTLINE_EMERALD;
    if (elements.btnPause) elements.btnPause.className = (primaryKey === 'pause') ? BTN_SOLID_INK : BTN_OUTLINE;
    if (elements.btnRetry) elements.btnRetry.className = (primaryKey === 'retry') ? BTN_SOLID_AMBER : BTN_OUTLINE_AMBER;
    if (elements.btnCancel) elements.btnCancel.className = BTN_OUTLINE;
    if (elements.btnReset) elements.btnReset.className = BTN_OUTLINE_RED;
    if (elements.btnLoad) elements.btnLoad.className = BTN_OUTLINE;

    _setDisabled(elements.btnValidate, !canValidate);
    _setDisabled(elements.btnProcess, !canProcess);
    _setDisabled(elements.btnMerge, !canMerge);
    _setDisabled(elements.btnPause, !canPause);
    _setDisabled(elements.btnRetry, !canRetry);
    _setDisabled(elements.btnCancel, !canNewTask);
    _setDisabled(elements.btnReset, !canDeleteTask);
    _setDisabled(elements.btnDownload, !canDownload);
    _setDisabled(elements.btnLoad, false);

    refreshWorkflowStepper(job);
}

export function switchTab(tabName, callback) {
    state.activeTab = tabName;
    elements.tabBtns.forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabName);
        btn.classList.toggle('text-slate-600', btn.dataset.tab === tabName);
        btn.classList.toggle('text-slate-400', btn.dataset.tab !== tabName);
    });
    elements.tabContents.forEach(content => {
        if (content.id === 'tab-' + tabName) {
            content.classList.remove('hidden');
            content.classList.add('block');
        } else {
            content.classList.add('hidden');
            content.classList.remove('block');
        }
    });
    if (callback) callback();
}

export function toggleApiKeyVisibility() {
    const el = elements.form.querySelector('[name="llm_api_key"]');
    if (!el) return;
    if (el.type === 'password') {
        el.type = 'text';
        elements.btnToggleApiKey.textContent = 'HIDE';
    } else {
        el.type = 'password';
        elements.btnToggleApiKey.textContent = 'SHOW';
    }
}

export function setLlmDirty(dirty) {
    if (!elements.llmDirtyHint) return;
    elements.llmDirtyHint.classList.toggle('hidden', !dirty);
    elements.llmDirtyHint.classList.toggle('inline', dirty);
    elements.btnSaveLlmDefaults.disabled = !dirty;
    elements.btnSaveLlmDefaults.classList.toggle('opacity-50', !dirty);
}

