from __future__ import annotations

import tempfile
from pathlib import Path

import novel_proofer.runner as runner
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.config import LLMConfig


def run_pipeline_for_text(
    input_text: str,
    fmt: FormatConfig,
    llm: LLMConfig,
    *,
    base_work_dir: Path,
    cleanup_debug_dir: bool = True,
) -> str:
    """Run the real runner pipeline and return final output text."""

    with tempfile.TemporaryDirectory(dir=str(base_work_dir)) as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"
        input_path = Path(td) / "in.txt"
        input_path.write_text(input_text, encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.update(
                job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=cleanup_debug_dir
            )
            runner.run_job(job_id, input_path, fmt, llm)
            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.state == "done", st.error
            return out_path.read_text(encoding="utf-8")
        finally:
            GLOBAL_JOBS.delete(job_id)
