from __future__ import annotations

import time
from pathlib import Path

import novel_proofer.runner as runner
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS


def test_merge_outputs_enforces_paragraph_indent_after_merge(tmp_path: Path) -> None:
    work_dir = tmp_path / "job"
    (work_dir / "out").mkdir(parents=True, exist_ok=True)
    out_path = tmp_path / "merged.txt"

    (work_dir / "out" / "000000.txt").write_text(
        "第2章 迷路\n　　她推开门说：“你怎么在这”\n他笑了笑：“路过”\n",
        encoding="utf-8",
    )

    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        fmt = FormatConfig(paragraph_indent=True, indent_with_fullwidth_space=True)
        GLOBAL_JOBS.update(job.job_id, work_dir=str(work_dir), output_path=str(out_path), format=fmt)
        GLOBAL_JOBS.init_chunks(job.job_id, total_chunks=1)
        GLOBAL_JOBS.update_chunk(job.job_id, 0, state="done", finished_at=time.time())
        GLOBAL_JOBS.update(job.job_id, phase="merge")

        runner.merge_outputs(job.job_id, cleanup_debug_dir=False)

        assert (
            out_path.read_text(encoding="utf-8")
            == "第2章 迷路\n\n　　她推开门说：“你怎么在这”\n\n　　他笑了笑：“路过”\n"
        )
    finally:
        GLOBAL_JOBS.delete(job.job_id)
