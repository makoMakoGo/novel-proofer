from __future__ import annotations

from novel_proofer.jobs import JobStatus
from novel_proofer.workflow import WorkflowContext


def workflow_context_for_job(st: JobStatus) -> WorkflowContext:
    if st.chunk_counts:
        return WorkflowContext.from_counts(
            state=st.state,
            phase=st.phase,
            wait_reason=st.wait_reason,
            total_chunks=st.total_chunks,
            chunk_counts=dict(st.chunk_counts),
        )
    return WorkflowContext.from_values(
        state=st.state,
        phase=st.phase,
        wait_reason=st.wait_reason,
        chunks=[chunk.state for chunk in st.chunk_statuses],
    )
