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
            st1 = GLOBAL_JOBS.get(job_id)
            assert st1 is not None
            assert st1.state == "paused", st1.error
            assert getattr(st1, "phase", None) == "process"

            runner.resume_paused_job(job_id, llm)
            st2 = GLOBAL_JOBS.get(job_id)
            assert st2 is not None
            assert st2.state == "paused", st2.error
            assert getattr(st2, "phase", None) == "merge"

            runner.merge_outputs(job_id)
            st3 = GLOBAL_JOBS.get(job_id)
            assert st3 is not None
            assert st3.state == "done", st3.error
            assert getattr(st3, "phase", None) == "done"
            return out_path.read_text(encoding="utf-8")
        finally:
            GLOBAL_JOBS.delete(job_id)
