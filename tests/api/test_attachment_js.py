from __future__ import annotations

import subprocess
import textwrap


def test_ui_attachment_helpers_are_browser_local_only() -> None:
    script = textwrap.dedent(
        """
        import assert from 'node:assert/strict';
        import {
            ATTACHED_JOB_KEY,
            clearPersistedAttachment,
            normalizeAttachedJobId,
            persistAttachment,
            readPersistedAttachment,
        } from './templates/static/js/attachment.js';

        const store = new Map();
        const storage = {
            getItem: (key) => store.has(key) ? store.get(key) : null,
            setItem: (key, value) => store.set(key, String(value)),
            removeItem: (key) => store.delete(key),
        };

        assert.equal(normalizeAttachedJobId('  abc123  '), 'abc123');
        assert.equal(readPersistedAttachment(storage), null);

        assert.equal(persistAttachment(storage, '  job-1  '), 'job-1');
        assert.equal(store.get(ATTACHED_JOB_KEY), 'job-1');
        assert.equal(readPersistedAttachment(storage), 'job-1');

        clearPersistedAttachment(storage);
        assert.equal(readPersistedAttachment(storage), null);

        assert.throws(() => persistAttachment(storage, '   '), /missing job_id/);
    """
    )

    subprocess.run(["node", "--input-type=module", "-e", script], check=True)
