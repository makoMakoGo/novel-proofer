# 测试用例说明

在仓库根目录执行 `.venv\Scripts\python.exe -m pytest --collect-only -q`，pytest 会收集到若干测试用例（包含参数化展开；具体数量以本地输出为准）。本文档按文件列出每个用例的覆盖点，便于快速定位“在测什么”。

如需运行全部测试：`pytest -q`。

注意：若设置 `NOVEL_PROOFER_RUN_LLM_TESTS=true`（或传入 `--run-llm-tests`），会额外运行标记为 `llm_integration` 的真实 LLM 集成测试。

## tests/api/test_endpoints.py

| Test case | 说明 |
| --- | --- |
| `tests/api/test_endpoints.py::test_healthz_ok` | 验证 `GET /healthz` 返回 `200` 且 JSON 为 `{"ok": True}`。 |
| `tests/api/test_endpoints.py::test_create_job_local_mode_writes_output_and_is_queryable` | 提供 LLM 配置创建任务，轮询等待完成；验证输出文件在 `OUTPUT_DIR` 下生成且内容非空，同时清理 `GLOBAL_JOBS` 记录避免串扰。 |
| `tests/api/test_endpoints.py::test_get_job_chunk_filter_and_paging` | 创建多分片任务后，用 `chunks=1&chunk_state=done&limit=1&offset=0` 拉取分片列表；验证分页 `has_more`、返回数量与 `chunk_counts.done` 统计。 |
| `tests/api/test_endpoints.py::test_job_not_found_error_envelope` | 查询不存在的任务时返回 `404`，并使用统一错误信封（`error.code == "not_found"`）。 |
| `tests/api/test_endpoints.py::test_invalid_job_id_returns_400_bad_request` | 非法 `job_id`（非 32 位 hex）应返回 `400`，并使用统一错误信封（`error.code == "bad_request"`）。 |
| `tests/api/test_endpoints.py::test_job_id_is_normalized_to_lowercase_for_lookup` | `job_id` 大小写不敏感：服务端应在路由层将 path 参数标准化为小写后再查询任务。 |
| `tests/api/test_endpoints.py::test_create_job_llm_enabled_requires_base_url_and_model` | LLM 配置缺失（`base_url/model` 为空）时，创建任务仍返回 `201`，但任务最终进入 `error` 状态。 |
| `tests/api/test_endpoints.py::test_job_actions_pause_resume` | 覆盖任务动作接口：`pause/resume` 的返回值与状态流转（通过 monkeypatch 避免真实 runner 副作用）。 |
| `tests/api/test_endpoints.py::test_pause_only_allowed_in_process_phase` | `pause` 仅允许在 `phase=process` 时执行；其他阶段返回 `409`。 |
| `tests/api/test_endpoints.py::test_reset_job_deletes_job` | 覆盖 `reset`：任务会从任务列表中被删除（但不会删除 `output/` 下已生成的最终输出）。 |
| `tests/api/test_endpoints.py::test_llm_settings_get_put_preserves_unknown_lines` | 覆盖 LLM 默认配置接口：`GET/PUT /api/v1/settings/llm`；验证写入 `.env` 时保留未知键/注释，并能读回保存的 LLM 字段。 |
| `tests/api/test_endpoints.py::test_rerun_all_creates_new_job_without_reupload` | 覆盖 `POST /api/v1/jobs/{job_id}/rerun-all`：基于输入缓存创建新任务并从头跑完整流程，且不需要重新上传文件。 |
| `tests/api/test_endpoints.py::test_job_input_stats_endpoint` | 覆盖 `GET /api/v1/jobs/{job_id}/input-stats`：基于输入缓存统计“非空白字符数”（UI 字数口径）。 |

## tests/formatting/test_chunking.py

| Test case | 说明 |
| --- | --- |
| `tests/formatting/test_chunking.py::test_chunk_by_lines_max_chars_non_positive_returns_whole_text` | `max_chars <= 0` 时不分片，原文整体作为单个 chunk 返回。 |
| `tests/formatting/test_chunking.py::test_chunk_by_lines_empty_text_returns_single_empty_chunk` | 空文本返回单个空 chunk（`[""]`）。 |
| `tests/formatting/test_chunking.py::test_chunk_by_lines_prefers_blank_line_break` | 触发分片时优先选择最近的空行边界进行切分。 |
| `tests/formatting/test_chunking.py::test_chunk_by_lines_flushes_at_boundary_when_over_budget` | 当预算恰好落在空行边界时应立即 flush，确保切分点稳定。 |
| `tests/formatting/test_chunking.py::test_chunk_by_lines_flushes_all_when_no_blank_line_available` | 没有空行可用时按行边界切分，避免产生超预算 chunk。 |
| `tests/formatting/test_chunking.py::test_chunk_by_lines_flush_upto_leaves_tail` | 保护内部 blank-line 跟踪：flush 后保留非空尾部，触发对剩余缓冲区的重新扫描。 |
| `tests/formatting/test_chunking.py::test_chunk_by_lines_with_first_chunk_max_uses_larger_budget_for_first_chunk` | 首 chunk 使用更大的 `first_chunk_max_chars` 预算，其余 chunk 使用 `max_chars`。 |

## tests/formatting/test_fixer.py

| Test case | 说明 |
| --- | --- |
| `tests/formatting/test_fixer.py::test_format_txt_llm_enabled_calls_llm_and_counts` | LLM 启用时会调用 LLM 路径，并在输出统计中计数（`llm_chunks == 1`）。 |
| `tests/formatting/test_fixer.py::test_format_txt_llm_enabled_keeps_front_matter_in_first_chunk_when_chunk_size_small` | 分片很小时，front-matter（作者/标签/简介）应仅出现在首 chunk（带 `FIRST_CHUNK_SYSTEM_PROMPT_PREFIX`），后续 chunk 不再包含这些字段。 |

## tests/formatting/test_rules.py

| Test case | 说明 |
| --- | --- |
| `tests/formatting/test_rules.py::test_apply_rules_all_transforms_and_stats` | 端到端覆盖全部本地规则：换行/行尾空白/空行/省略号/破折号/中文标点/标点间距/引号/段落缩进，并验证各 stats 计数为正。 |
| `tests/formatting/test_rules.py::test_apply_rules_fullwidth_indent` | 段首缩进启用“全角空格”模式时，输出以 `\u3000\u3000` 开头且统计计数更新。 |
| `tests/formatting/test_rules.py::test_paragraph_indent_mid_para_no_indent` | 同一段落的“中间行”（前一行非空）不应再次缩进，防止长段落被 LLM 断行后出现重复缩进。 |
| `tests/formatting/test_rules.py::test_paragraph_indent_after_blank_line` | 空行后的新段落起始行必须缩进，同时保留空行本身。 |

## tests/jobs/test_store.py

| Test case | 说明 |
| --- | --- |
| `tests/jobs/test_store.py::test_job_store_update_respects_started_at_and_pause_rules` | `started_at` 只接受首次写入；暂停状态不应被 `update(state="running")` 覆盖；进入终态（`done`）后清除 paused 标记。 |
| `tests/jobs/test_store.py::test_job_store_update_chunk_tracks_done_chunks` | `done_chunks` 随分片状态在 `done/pending` 间切换而增减；越界 index 更新应被忽略。 |
| `tests/jobs/test_store.py::test_job_store_add_retry_updates_job_and_chunk` | `add_retry()` 同时更新 job 级与 chunk 级重试/错误信息；无效 index 仍应累加 job 级计数。 |
| `tests/jobs/test_store.py::test_job_store_cancel_resets_processing_chunks` | 触发“删除任务（reset）”的终止信号后：任务变为 `cancelled` 且写入 `finished_at`，正在处理/重试的 chunk 重置为 `pending` 并清空时间戳。 |
| `tests/jobs/test_store.py::test_job_store_pause_resume_and_delete` | `pause/resume/delete` 的幂等性与返回值（重复操作返回 `False`）以及 paused 状态开关。 |
| `tests/jobs/test_store.py::test_job_store_ignores_unknown_jobs_and_cancelled_updates` | 对未知 job 的操作应无副作用；对已标记为 `cancelled` 的 job 的 `update()/update_chunk()` 应 no-op，避免状态被“复活”。 |
| `tests/jobs/test_store.py::test_job_store_persistence_is_throttled_and_flushable` | 持久化写盘不应发生在每次 `update_chunk()` 的热路径；dirty 更新应被节流并可通过 `flush_persistence()` 主动触发落盘。 |

## tests/llm/test_client.py

| Test case | 说明 |
| --- | --- |
| `tests/llm/test_client.py::test_llm_config_removed_retry_fields` | 校验 `LLMConfig` 已移除旧重试字段，且传入旧参数会抛 `TypeError`。 |
| `tests/llm/test_client.py::test_call_llm_text_routes_to_openai_compatible` | `call_llm_text()` 应路由到 `_call_openai_compatible()`。 |
| `tests/llm/test_client.py::test_call_openai_compatible_payload_has_no_max_tokens_by_default` | OpenAI-compatible 请求默认不带 `max_tokens`；使用 SSE `stream=True`，消息包含 system/user，且正确注入 `Authorization`。 |
| `tests/llm/test_client.py::test_call_openai_compatible_merges_extra_params` | `extra_params`（如 `max_tokens/temperature`）应合并进最终 payload。 |
| `tests/llm/test_client.py::test_parse_sse_line[data: [DONE]-expected0]` | SSE 行 `data: [DONE]` 解析为 done 信号（`("done", "")`）。 |
| `tests/llm/test_client.py::test_parse_sse_line[data:-expected1]` | SSE 行 `data:`（空内容）解析为 `("data", "")`。 |
| `tests/llm/test_client.py::test_parse_sse_line[data:  {"x":1}-expected2]` | SSE 行 `data:  {"x":1}` 去除前缀与多余空格后返回 `("data", "{\"x\":1}")`。 |
| `tests/llm/test_client.py::test_parse_sse_line[event: ping-None]` | SSE 的 `event:` 行不作为数据帧处理，应返回 `None`。 |
| `tests/llm/test_client.py::test_parse_sse_line[-None]` | 空行应返回 `None`。 |
| `tests/llm/test_client.py::test_is_loopback_host` | `_is_loopback_host()` 对 `localhost/127.0.0.1` 返回 `True`，其他地址返回 `False`。 |
| `tests/llm/test_client.py::test_httpx_client_for_url_bypasses_env_proxy_for_loopback` | loopback 请求应绕过环境代理：使用 `httpx.Client(trust_env=False)`。 |
| `tests/llm/test_client.py::test_stream_request_parses_openai_sse` | SSE 流式响应中 `choices[].delta.content` 需按顺序拼接，直到 `[DONE]`。 |
| `tests/llm/test_client.py::test_stream_request_stops_reading_after_done` | 读到 `[DONE]` 后必须停止继续 `read()`（防止额外 IO/异常）。 |
| `tests/llm/test_client.py::test_stream_request_should_stop_short_circuits` | `should_stop()` 为真时应短路并抛出终止错误（`LLMError("cancelled")`）。 |
| `tests/llm/test_client.py::test_stream_request_wraps_url_error` | 网络层 `httpx.RequestError` 需被包装为 `LLMError("LLM request failed: ...")`。 |
| `tests/llm/test_client.py::test_http_post_json_success` | `_http_post_json()` 能成功解析 JSON 响应体为 Python dict。 |
| `tests/llm/test_client.py::test_http_post_json_wraps_url_error` | `_http_post_json()` 遇到 `httpx.RequestError` 需转换为 `LLMError`。 |
| `tests/llm/test_client.py::test_call_llm_text_resilient_retries_and_succeeds` | `call_llm_text_resilient()` 对可重试错误（如 `HTTP 500`）进行多次尝试并最终成功；同时会调用 `sleep()` 退避。 |
| `tests/llm/test_client.py::test_call_llm_text_resilient_non_retryable_raises` | 对不可重试错误（如 `HTTP 400`）不应退避重试，直接抛出异常。 |
| `tests/llm/test_client.py::test_call_llm_text_resilient_with_meta_calls_on_retry` | 带 meta 的重试接口会返回 `retries/last_code/last_msg`，并在重试时触发 `on_retry(idx, code, msg)` 回调。 |

## tests/pipeline/test_corpus_golden.py

| Test case | 说明 |
| --- | --- |
| `tests/pipeline/test_corpus_golden.py::test_pipeline_corpus_golden` | 离线 golden：从 `tests/cases/pipeline/*/` 读取 `input.txt`，走真实 per-chunk + merge 流程；用 fake LLM（回显输入）保证稳定；默认断言 `expected.txt` 精确一致（可用 `--update-golden` 更新）。 |

## tests/llm/test_corpus_integration.py

| Test case | 说明 |
| --- | --- |
| `tests/llm/test_corpus_integration.py::test_llm_corpus_end_to_end_invariants` | 可选 LLM 端到端 invariants：读取 `tests/cases/llm/*/`，调用真实 OpenAI-compatible endpoint 跑完整 per-chunk + merge；断言一组不变量与每个 case 的 `assertions`（不再做全量 golden 字符串对比）。失败时会把输入/输出写到 `tests/.artifacts/llm_corpus/<case>/` 便于对比。 |

## tests/runner/test_blank_chunk.py

| Test case | 说明 |
| --- | --- |
| `tests/runner/test_blank_chunk.py::test_llm_worker_skips_whitespace_only_chunk` | 输入 chunk 仅包含空白时，`_llm_worker()` 不应调用 LLM：直接回写原文并标记 chunk 为 done，同时不生成 `resp/req/error` 目录。 |

## tests/runner/test_chunk_newlines.py

| Test case | 说明 |
| --- | --- |
| `tests/runner/test_chunk_newlines.py::test_align_leading_blank_lines_restores_missing_blank_lines` | LLM 输出丢失开头空行时，`_align_leading_blank_lines()` 需补回缺失空行。 |
| `tests/runner/test_chunk_newlines.py::test_align_leading_blank_lines_trims_excess_blank_lines` | LLM 输出多余开头空行时应裁剪到与输入一致。 |
| `tests/runner/test_chunk_newlines.py::test_align_leading_blank_lines_treats_whitespace_lines_as_blank` | 开头由空格/制表符构成的“空行”也应被视作空行并对齐。 |
| `tests/runner/test_chunk_newlines.py::test_align_trailing_newlines_restores_missing_blank_line` | LLM 输出丢失段落空行时，`_align_trailing_newlines()` 需补回缺失的空行。 |
| `tests/runner/test_chunk_newlines.py::test_align_trailing_newlines_adds_missing_newline` | LLM 输出缺少行尾换行符时应补齐。 |
| `tests/runner/test_chunk_newlines.py::test_align_trailing_newlines_trims_excess_newlines` | LLM 输出多余换行符时应裁剪到与输入一致。 |
| `tests/runner/test_chunk_newlines.py::test_align_trailing_newlines_normalizes_crlf` | 输入/输出存在 CRLF 时，需先统一为 `\n` 再对齐末尾换行。 |
| `tests/runner/test_chunk_newlines.py::test_merge_chunk_outputs_inserts_blank_line_between_chunks` | 合并相邻 chunk 时若两端均为非空行，应在 chunk 边界插入一个空行避免段落粘连。 |
| `tests/runner/test_chunk_newlines.py::test_merge_chunk_outputs_inserts_blank_line_within_single_chunk` | 单个 chunk 内若出现相邻非空行，合并逻辑同样会在两行间补一个空行。 |

## tests/runner/test_extra_coverage.py

| Test case | 说明 |
| --- | --- |
| `tests/runner/test_extra_coverage.py::test_llm_worker_records_retries_and_aligns_newlines` | LLM 路径下：记录重试次数与错误码；在启用 raw 响应保留时写入 `resp/`；并对齐输出尾部换行（如补空行）。 |
| `tests/runner/test_extra_coverage.py::test_llm_worker_cancel_behaviors` | 覆盖多种终止时机：开始前/LLM 返回后/异常处理中触发终止时，worker 应提前退出且避免落盘副作用。 |
| `tests/runner/test_extra_coverage.py::test_llm_worker_ratio_validation_errors` | 输出长度校验：除首 chunk 外，过短/过长输出应标记 chunk 为 error 并写入错误信息。 |
| `tests/runner/test_extra_coverage.py::test_run_llm_for_indices_paused_cancelled_and_worker_exception` | `_run_llm_for_indices()` 在 job paused/cancelled 时直接返回；worker 抛异常时不应导致整体失败（最终返回 done）。 |
| `tests/runner/test_extra_coverage.py::test_run_job_cancellation_llm_outcomes_and_exception` | `run_job()` 对终止的多阶段分支（预处理/输出）与 LLM outcome（paused/cancelled）处理正确；异常应把 job 置为 error 并记录信息。 |
| `tests/runner/test_extra_coverage.py::test_retry_failed_and_resume_paused_branches` | 覆盖 `retry_failed_chunks()/resume_paused_job()` 的缺失配置分支、无失败分支、以及 paused/cancelled outcome 分支与收尾 done 分支。 |

## tests/runner/test_jobs_logs.py

| Test case | 说明 |
| --- | --- |
| `tests/runner/test_jobs_logs.py::test_llm_worker_success_writes_resp_only_when_enabled` | 成功时默认不写 `resp/`；启用保留时仅写入按 index 命名的 `resp/000000.txt`，且不产生旧的 `req/`、`error/` 目录。 |
| `tests/runner/test_jobs_logs.py::test_llm_worker_error_does_not_create_error_dir` | 失败时不创建 `error/` 目录，且 chunk 状态应为 error 并记录 `last_error_code/message`。 |
| `tests/runner/test_jobs_logs.py::test_retry_failed_chunks_overwrites_resp` | 失败后重试应覆盖旧 `resp` 文件并最终写出合并输出，同时 job/chunk 状态收敛为 done。 |
| `tests/runner/test_jobs_logs.py::test_resume_paused_job_overwrites_existing_resp` | 恢复暂停任务时，如已有旧 `resp`，也应被覆盖为新响应并生成最终输出。 |

## tests/runner/test_run_job.py

| Test case | 说明 |
| --- | --- |
| `tests/runner/test_run_job.py::test_run_job_missing_paths_sets_error` | `run_job()` 若 job 缺少 `work_dir/output_path` 必须置为 error 并提供错误信息。 |
| `tests/runner/test_run_job.py::test_run_job_local_mode_cleans_up_by_default` | 本地模式（LLM 关闭）默认在任务 done 后清理调试目录（`cleanup_debug_dir=True`）。 |
| `tests/runner/test_run_job.py::test_run_job_local_mode_keeps_debug_dir_when_opted_out` | 显式关闭清理时应保留调试目录结构（`README.txt/pre/out`），且不应生成 `req/`、`error/` 目录。 |

## tests/api/test_server_utils.py

| Test case | 说明 |
| --- | --- |
| `tests/api/test_server_utils.py::test_safe_filename_and_derive_output_filename` | `_safe_filename()` 防空与文件名清理；`_derive_output_filename()` 负责后缀/扩展名推导且空后缀走默认 `_rev`。 |
| `tests/api/test_server_utils.py::test_decode_text_prefers_utf8_sig` | `_decode_text()` 优先处理 UTF-8 BOM（`\xef\xbb\xbf`），输出不包含 BOM。 |
| `tests/api/test_server_utils.py::test_cleanup_job_dir_validation_and_removal` | `_cleanup_job_dir()` 校验 job_id 格式；目录不存在返回 `False`，存在则删除并返回 `True`。 |
| `tests/api/test_server_utils.py::test_server_main_parses_and_calls_uvicorn` | `server.main()` 能正确解析 CLI 参数并调用 `uvicorn.run`（host/port/log_level/reload）。 |

## tests/llm/test_think_filter.py

| Test case | 说明 |
| --- | --- |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_no_think_tags` | 无 think 标签时流式输出应原样透传。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_simple_think_tag` | 过滤单个 `<think>...</think>` 后只保留可见输出。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_think_tag_case_insensitive` | think 标签大小写不敏感（`<THINK>`、`<Think>` 等均可识别）。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_think_tag_with_content_before_and_after` | think 标签前后的文本应被保留且拼接正确。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_multiple_think_tags` | 多个 think 段落应全部被过滤。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_nested_think_tags` | 支持嵌套 think 标签并按贪婪方式匹配过滤。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_cross_chunk_open_tag` | opening tag 跨 chunk 分割时仍能正确识别并过滤。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_cross_chunk_close_tag` | closing tag 跨 chunk 分割时仍能正确识别并过滤。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_cross_chunk_content` | think 内容跨多个 chunk 时仍能正确过滤。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_incomplete_open_tag_at_end` | 尾部残缺的 `<` 不应误判为标签并被误删。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_unclosed_think_tag` | 未闭合的 think 标签应过滤其后所有内容（输出为空）。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_reset` | `reset()` 能清理内部状态，允许同一实例复用。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_empty_think_tag` | 空的 think 标签也应被过滤，不影响前后内容。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_multiline_think_content` | think 标签内的多行内容应整体被过滤。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_empty_chunk_returns_empty_string` | `feed("")` 返回空字符串，不产生副作用。 |
| `tests/llm/test_think_filter.py::TestThinkTagFilter::test_nested_open_without_close_in_chunk_increments_depth` | 单个 chunk 内出现多层 opening tag 且无 closing 时应保持过滤态（flush 输出为空）。 |
| `tests/llm/test_think_filter.py::TestFilterThinkTagsFunction::test_simple_filter` | `filter_think_tags()` 一次性调用可正确过滤 think 内容。 |
| `tests/llm/test_think_filter.py::TestFilterThinkTagsFunction::test_no_tags` | 无标签输入调用 `filter_think_tags()` 应原样返回。 |
| `tests/llm/test_think_filter.py::TestFilterThinkTagsFunction::test_complex_content` | 复杂输入（多标签/大小写混用）可正确过滤并拼接剩余文本。 |
| `tests/llm/test_think_filter.py::TestFilterThinkTagsFunction::test_unclosed_think_tag_filters_trailing_content` | 未闭合标签会导致尾部内容被过滤，只保留前缀。 |
| `tests/llm/test_think_filter.py::TestMaybeFilterThinkTags::test_unclosed_returns_raw_stripped_tags` | `_maybe_filter_think_tags()` 遇到未闭合标签时回退为“仅去标签标记、保留内容”。 |
| `tests/llm/test_think_filter.py::TestMaybeFilterThinkTags::test_balanced_filters` | 标签闭合且输出比例正常时，优先执行真正的过滤（去掉 think 内容）。 |
| `tests/llm/test_think_filter.py::TestMaybeFilterThinkTags::test_balanced_filters_can_fall_back_to_stripping` | 当过滤后输出相对输入过短时回退为“去标记保内容”（避免误删）。 |
| `tests/llm/test_think_filter.py::TestMaybeFilterThinkTags::test_filtering_is_always_on_does_not_return_raw` | think 标签过滤为强制开启：遇到 think 标签时不会原样返回（至少会过滤或去标记）。 |
| `tests/llm/test_think_filter.py::TestMaybeFilterThinkTags::test_low_output_ratio_falls_back_to_stripping_tags` | 当输出比例过低（疑似误删/边界情况）时回退为“去标记保内容”。 |
