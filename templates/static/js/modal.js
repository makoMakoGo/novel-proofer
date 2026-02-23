
// Modal logic

let modalCloseCallback = null;
let modalCloseTimer = null;
let modalToken = 0;

function getElements() {
    return {
        backdrop: document.getElementById('modalBackdrop'),
        box: document.getElementById('modalBox'),
        title: document.getElementById('modalTitle'),
        body: document.getElementById('modalBody'),
        actions: document.getElementById('modalActions'),
    };
}

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

export function openModal() {
    const el = getElements();
    if (!el.backdrop) return;

    modalToken += 1;
    if (modalCloseTimer) {
        clearTimeout(modalCloseTimer);
        modalCloseTimer = null;
    }

    el.backdrop.classList.remove('hidden');
    el.backdrop.classList.add('flex');
    requestAnimationFrame(() => {
        el.box.classList.remove('scale-95', 'opacity-0');
        el.box.classList.add('scale-100', 'opacity-100');
    });
}

export function closeModal() {
    const el = getElements();
    if (!el.backdrop) return;

    el.box.classList.remove('scale-100', 'opacity-100');
    el.box.classList.add('scale-95', 'opacity-0');

    const token = modalToken;
    const cb = modalCloseCallback;
    modalCloseCallback = null;

    if (modalCloseTimer) clearTimeout(modalCloseTimer);
    modalCloseTimer = setTimeout(() => {
        modalCloseTimer = null;
        if (token !== modalToken) return;
        el.backdrop.classList.add('hidden');
        el.backdrop.classList.remove('flex');
        if (cb) cb();
    }, 200);
}

export function initModal() {
    const el = getElements();
    if (!el.backdrop) return;
    
    el.backdrop.addEventListener('click', (e) => {
        if (e.target === el.backdrop) closeModal();
    });
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && !el.backdrop.classList.contains('hidden')) closeModal();
    });
}

export function showConfirm(message) {
    return new Promise((resolve) => {
        const el = getElements();
        el.title.className = 'text-base font-medium text-slate-800 mb-3';
        el.title.textContent = '确认操作';
        el.body.innerHTML = `<p class="whitespace-pre-wrap">${message}</p>`;
        el.actions.innerHTML = `
          <button type="button" class="modal-cancel px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-800 transition-colors">取消</button>
          <button type="button" class="modal-confirm px-4 py-2 bg-ink text-white text-sm font-medium rounded-lg hover:bg-zinc-800 transition-colors">确认</button>
        `;
        el.actions.className = 'flex justify-end gap-2';

        modalCloseCallback = () => resolve(false);
        el.actions.querySelector('.modal-cancel').onclick = () => { closeModal(); resolve(false); };
        el.actions.querySelector('.modal-confirm').onclick = () => { closeModal(); resolve(true); };
        openModal();
    });
}

export function showJobPicker(jobs, { hasCurrentJob = false } = {}) {
    return new Promise((resolve) => {
        const el = getElements();
        const purgeLabel = hasCurrentJob ? '清理其他' : '清理全部';
        el.title.className = 'text-base font-medium text-slate-800 mb-3 flex items-center justify-between';
        el.title.innerHTML = `<span>选择要加载的任务</span><button type="button" class="modal-purge-btn text-xs font-normal text-red-500 hover:text-red-600 transition-colors">${purgeLabel}</button>`;
        
        const listHtml = jobs.slice(0, 20).map((j) => {
            const jobId = escapeHtml(j?.id ? String(j.id) : '');
            const id8 = escapeHtml(j?.id ? String(j.id).slice(0, 8) : '-');
            const st = escapeHtml(j?.state || '-');
            const phase = escapeHtml(j?.phase || '-');
            const prog = escapeHtml(`${j?.progress?.done_chunks || 0}/${j?.progress?.total_chunks || 0}`);
            const name = escapeHtml(j?.input_filename ? String(j.input_filename) : '');
            
            const stColor = { 
                done: 'bg-emerald-100 text-emerald-700', 
                error: 'bg-red-100 text-red-700', 
                paused: 'bg-amber-100 text-amber-700', 
                cancelled: 'bg-slate-200 text-slate-600' 
            }[j?.state] || 'bg-slate-100 text-slate-600';

            return `<button type="button" data-job-id="${jobId}" class="job-item w-full text-left px-3 py-2 rounded-lg hover:bg-slate-50 border border-slate-100 hover:border-slate-200 transition-colors flex items-center gap-3">
            <span class="font-mono text-xs text-slate-400">${id8}</span>
            <span class="text-xs px-1.5 py-0.5 rounded ${stColor}">${st}:${phase}</span>
            <span class="text-xs text-slate-500 font-mono">${prog}</span>
            <span class="text-sm text-slate-700 truncate flex-1">${name}</span>
          </button>`;
        }).join('');
        
        el.body.innerHTML = `
          <div class="space-y-2 max-h-64 overflow-y-auto mb-4">${listHtml || '<p class="text-slate-400 text-sm">暂无可加载任务</p>'}</div>
          <div class="flex items-center gap-2 pt-3 border-t border-slate-100">
            <input type="text" id="manualJobId" placeholder="或输入完整 job_id" class="flex-1 px-3 py-1.5 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-ink/20 focus:border-ink font-mono" />
            <button type="button" class="modal-manual px-3 py-1.5 text-sm font-medium text-slate-600 hover:text-ink transition-colors">加载</button>
          </div>
        `;
        
        el.actions.innerHTML = `<button type="button" class="modal-cancel px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-800 transition-colors">取消</button>`;
        el.actions.className = 'flex justify-end';
        
        modalCloseCallback = () => resolve(null);
        el.actions.querySelector('.modal-cancel').onclick = () => { closeModal(); resolve(null); };
        const purgeBtn = el.title.querySelector('.modal-purge-btn');
        if (purgeBtn) purgeBtn.onclick = () => { closeModal(); resolve('__purge_all__'); };
        
        el.body.querySelectorAll('.job-item').forEach(btn => {
            btn.onclick = () => { closeModal(); resolve(btn.dataset.jobId); };
        });
        
        const manualBtn = el.body.querySelector('.modal-manual');
        const manualInput = el.body.querySelector('#manualJobId');
        
        if (manualBtn && manualInput) {
            manualBtn.onclick = () => {
                const val = manualInput.value.trim();
                if (val && val !== '__purge_all__') { closeModal(); resolve(val); }
            };
            manualInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    const val = e.target.value.trim();
                    if (val && val !== '__purge_all__') { closeModal(); resolve(val); }
                }
            });
        }
        
        openModal();
    });
}
