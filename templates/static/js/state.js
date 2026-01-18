
// State management

export const state = {
    currentJobId: null,
    currentJobState: null,
    currentJobPhase: null,
    currentFilter: 'all',
    activeTab: 'progress',
    chunksData: [],
    chunkCounts: null,
    totalChunksFromServer: 0,
    
    // Flags
    forceChunksFetch: false,
    chunksFetchInFlight: false,
    createJobInFlight: false,
    pollInFlight: false,
    
    // Timers
    pollTimer: null,
    pollJobId: null,

    // Caches
    inputCharsCache: new Map(),
    inputCharsInFlight: new Set(),
    llmSavedSnapshot: null,
    
    // Constants
    INPUT_CHARS_MISSING: Symbol('INPUT_CHARS_MISSING'),
    INPUT_CHARS_ERROR: Symbol('INPUT_CHARS_ERROR'),
};

const UI_STATE_KEY = 'novel_proofer.ui_state.v1';
const ATTACHED_JOB_KEY = 'novel_proofer.attached_job_id.v2';

export const UI_STATE_FIELDS = [
    'suffix',
    'max_chunk_chars',
    'paragraph_indent',
    'indent_with_fullwidth_space',
    'normalize_blank_lines',
    'trim_trailing_spaces',
    'normalize_ellipsis',
    'normalize_em_dash',
    'normalize_cjk_punctuation',
    'fix_cjk_punct_spacing',
    'normalize_quotes',
    'cleanup_debug_dir',
];

export function loadUiState(formElement) {
    try {
        const raw = localStorage.getItem(UI_STATE_KEY);
        if (!raw) return;
        const saved = JSON.parse(raw);
        if (!saved || typeof saved !== 'object') return;
        
        for (const name of UI_STATE_FIELDS) {
            let el;
            if (name === 'cleanup_debug_dir') {
                 el = formElement.querySelector('input[type="checkbox"][name="cleanup_debug_dir"]');
            } else {
                 el = formElement.querySelector(`[name="${name}"]`);
            }
            
            if (!el) continue;
            if (el.type === 'checkbox') {
                el.checked = !!saved[name];
            } else if (saved[name] != null) {
                el.value = String(saved[name]);
            }
        }
    } catch (e) {}
}

export function saveUiState(formElement) {
    try {
        const saved = {};
        for (const name of UI_STATE_FIELDS) {
            let el;
            if (name === 'cleanup_debug_dir') {
                 el = formElement.querySelector('input[type="checkbox"][name="cleanup_debug_dir"]');
            } else {
                 el = formElement.querySelector(`[name="${name}"]`);
            }
            if (!el) continue;
            saved[name] = (el.type === 'checkbox') ? !!el.checked : String(el.value || '');
        }
        localStorage.setItem(UI_STATE_KEY, JSON.stringify(saved));
    } catch (e) {}
}

export function getAttachedJobId() {
    try {
        return localStorage.getItem(ATTACHED_JOB_KEY);
    } catch (e) {
        return null;
    }
}

export function setAttachedJobId(id) {
    try {
        if (id) localStorage.setItem(ATTACHED_JOB_KEY, id);
        else localStorage.removeItem(ATTACHED_JOB_KEY);
    } catch (e) {}
}

export function saveActiveTab(tabName) {
    localStorage.setItem('activeTab', tabName);
}

export function getSavedActiveTab() {
    return localStorage.getItem('activeTab') || 'progress';
}
