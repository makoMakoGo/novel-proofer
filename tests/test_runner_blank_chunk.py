from __future__ import annotations

import tempfile
from pathlib import Path

from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.config import LLMConfig
from novel_proofer.runner import _llm_worker


def test_llm_worker_skips_whitespace_only_chunk() -> None:
    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td)
        (work_dir / "pre").mkdir(parents=True, exist_ok=True)

        pre = "\n\n\n"
        (work_dir / "pre" / "000000.txt").write_text(pre, encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.init_chunks(job_id, total_chunks=1)

            # Base URL/model intentionally empty: if LLM call happens, it would error.
            cfg = LLMConfig(enabled=True, provider="openai_compatible", base_url="", model="")

            _llm_worker(job_id, 0, work_dir, cfg)

            out = (work_dir / "out" / "000000.txt").read_text(encoding="utf-8")
            assert out == pre

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.done_chunks == 1
            assert st.chunk_statuses[0].state == "done"
            assert st.chunk_statuses[0].input_chars == len(pre)
            assert st.chunk_statuses[0].output_chars == len(pre)
        finally:
            GLOBAL_JOBS.delete(job_id)

