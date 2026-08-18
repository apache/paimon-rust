# Vindex range-read 双限流优化计划

> 状态（2026-08-18）：已在 `perf/vindex-range-read-concurrency` 工作树实施，尚未提交。本文件现作为实施与验收记录；基线 commit 仍为 `1e172b3`，下列 Task 6 A/B 尚待执行。

## 1. 结论

当前 `1e172b3` 已把响应内存从“保留全部 range”降为有界，但同一个 semaphore 同时覆盖底层 I/O、channel 排队和请求 buffer 复制。热 cache 下读取很快，permit 的主要持有时间会转移到交付和复制阶段，因此真实 I/O 不能及时补位；这与 4 GiB 热 cache 测试中约 3.4% 的 QPS 回退方向一致。

采用两个独立限制是正确方向：

- I/O limit：`C = global-index.range-read-thread-num`，只覆盖 `FileRead::read(...).await`。
- Response limit：内部固定为 `min(2C, Semaphore::MAX_PERMITS)`，覆盖从发起读取前到响应被消费完成的生命周期。
- Channel capacity：`C`，允许已完成响应批量交给同步消费者。

但不能只把第二个 semaphore 和 channel 加到当前实现。当前 producer 在 `buffer_unordered(C)` 外逐个 `sender.send(...).await`；channel 满时 producer 停止轮询整个 stream，其他已就绪 read future 也不能及时完成和释放 I/O permit。应把 `send` 放进每个受限 work item，并让 work window 使用 response limit（`2C`），再由 I/O semaphore 单独限制真实读取为 `C`。

### 1.1 新增 benchmark 证据（2026-08-17）

| 实现 | Warmup | 50q 耗时 | QPS | Recall | NDCG |
|---|---:|---:|---:|---:|---:|
| 旧版 `0946fcd` | 3.669s | 3.105s | 16.101 | 0.8182 | 0.86104 |
| 标记为新版 `ab90d01` | 3.810s | 3.761s | 13.295 | 0.8182 | 0.86104 |

该样本中新版 Warmup 慢 3.8%，50q 耗时增加 21.1%，QPS 下降 17.4%，已经超过单轮约 3% 的噪声容忍范围，必须作为性能回退候选处理；Recall/NDCG 一致，说明不是搜索参数或结果质量变化换来的差异。

不过 `ab90d01` 是当前 PR 的 main 基线，不是当前 feature HEAD `1e172b3`。正式归因前必须从编译日志或 binary provenance 确认实际源码 commit，不能只相信结果 JSON/表格中的硬编码字段。Lance 的 Recall/NDCG 与 Paimon 不同，只作为外部吞吐参考，不用于计算本 PR 的回退幅度。

## 2. 必须保持的边界

1. 两个 semaphore 都必须在一次 vector search 的 reader 创建点生成，并被所有 index reader 和 clone 共享；不得按 file、clone 或 `pread` 单独创建，否则总响应内存会随 reader 数放大。
2. 获取顺序固定为 response permit -> I/O permit。反过来会让任务持有稀缺 I/O permit 等待内存预算。
3. `FileRead::read` 返回后立即释放 I/O permit；长度校验、channel 等待和请求 buffer 复制不得占用它。
4. Response permit 随 `(index, Bytes)` 穿过 channel，在 `consume` 返回后释放；错误、receiver 关闭和 future 取消也必须依靠 RAII 释放。
5. `range_permit_wait_nanos` 继续只统计 I/O permit 等待，不混入 response backpressure；`peak_in_flight_reads` 继续由 `InFlightRead` 统计真实底层读取。
6. 该限制按响应个数而不是字节数计量。瞬时 multi-range 响应为 `O(2C * max_response_size)`；请求方 buffer 和既有的每-reader 64 KiB scalar read-ahead cache 不在此预算内。
7. `2C` 是吞吐/内存折中，不保证任何时刻严格跑满 `C` 路 I/O。消费者、channel 和等待发送的响应耗尽窗口时会自然背压；不为理论上的单槽气泡引入 `2C+1` 或第三个配置，除非 benchmark 证明有必要。

## 3. 修改范围

只修改：

- `crates/paimon/src/vindex/range_reader.rs`
- `crates/paimon/src/table/vector_search_builder.rs`

不修改配置、SQL 文档、vindex-core、合并规则或现有诊断输出格式；不增加依赖和用户参数。

## 4. 实施步骤

### Task 0：锁定基线（已完成）

实施基线为分支 `perf/vindex-range-read-concurrency` 的 `1e172b3`。当前工作树已有本计划的未提交实现，因此不再预期干净状态。

```bash
git status --short --branch
git rev-parse HEAD
```

实施前确认 HEAD 为 `1e172b3`；当前两个目标文件的修改即本计划候选实现。

### Task 1：行为测试（已完成，实施后对账）

文件：`crates/paimon/src/vindex/range_reader.rs` 的现有 tests 模块。

1. `response_copy_and_buffers_are_bounded_across_clones` 使用 `C=1`，让第一个 reader 的响应进入 `consume` 后阻塞，验证第二个底层 read 已启动，同时另一个 clone 的第 `2C+1` 个 read 在释放 response permit 前不会启动、释放后能够继续。
2. 该测试合并覆盖“复制不持有 I/O permit”和“所有 clone 全局共享 2C 响应上限”，避免两套重复测试样板。
3. 正向同步使用 semaphore/channel；“第 `2C+1` 个 read 尚未启动”是必要的否定断言，明确允许使用 1 秒 timeout 作为例外。其余 timeout 只用于防止 CI 永久挂起，不作为性能阈值。

本计划与已有工作树对账时生产实现已经存在，无法在当前状态重放 RED-first；这里记录为实施后的回归/characterization test，不伪造 RED 结果。

验证：

```bash
cargo test -p paimon vindex::range_reader::tests::response_copy_and_buffers_are_bounded_across_clones
```

### Task 2：把两个限制绑定为一个共享内部值（已完成）

文件：`crates/paimon/src/vindex/range_reader.rs`。

1. 用一个最小的 `pub(crate) RangeReadLimiter` 绑定 shared I/O semaphore、shared response semaphore、`io_limit=C` 和 `response_limit=min(C.saturating_mul(2), Semaphore::MAX_PERMITS)`。
2. `VindexFileReader` 持有 limiter，替换当前分离的 `permits + max_range_read_concurrency`，避免构造时传入互相矛盾的 semaphore size 和并发值。
3. 测试构造器仍用默认 `C=32`；`try_clone_reader` clone 同一个 limiter 内的两个 `Arc<Semaphore>`。
4. `response_limit_saturates_at_semaphore_max_permits` 覆盖 `C > MAX_PERMITS / 2` 时不会乘法溢出或让 `Semaphore::new` panic。

验证：现有 `clones_share_range_read_permits` 更新为同时断言 I/O 与 response semaphore 被共享；不增加通用 limiter trait、factory 或新配置。

### Task 3：在两个生产入口全局共享 limiter（已完成）

文件：`crates/paimon/src/table/vector_search_builder.rs`。

在以下两个现有 semaphore 创建点各创建一次 `RangeReadLimiter::new(range_read_concurrency)`，再 clone 给所有 Vindex reader：

- `plan_and_search_pk_candidates_batch` 的 loader 路径；
- `evaluate_batch_vector_search` 的 vector entry 路径。

Lumina 路径保持不变。`global-index.thread-num`、native batch parallelism 和 `global-index.range-read-thread-num` 的现有语义保持不变。

验证：`rg -n "RangeReadLimiter::new" crates/paimon/src/table/vector_search_builder.rs` 只应命中上述两个 search scope，不能出现在 per-file closure 内。

### Task 4：重排 fetch pipeline（已完成）

文件：`crates/paimon/src/vindex/range_reader.rs` 的 `fetch_range_batch`。

每个 range work item 按以下顺序执行：

```text
acquire response permit
  -> acquire I/O permit
  -> FileRead::read
  -> drop InFlightRead and I/O permit
  -> validate response
  -> bounded channel send
  -> synchronous consume/copy
  -> drop response permit
```

具体要求：

1. channel capacity 从 `1` 改为 `C`。
2. 把 `sender.send` 移进每个 range future；future 完成发送后才算 work item 完成。
3. `buffer_unordered` 的 work window 使用 `response_limit`，真实读取仍由 I/O semaphore 限制为 `C`。
4. 第一个 read/send 错误送达 consumer 后停止 producer；drop 剩余 stream，取消未完成 future 并释放两个 permit。
5. `FileRead::read(...).await` 的结果先保存，再立即 drop `InFlightRead` 和 I/O permit，然后执行错误映射、长度校验和发送。
6. 不改变 completion-order copy、range merge、scalar cache 或 stats 字段。

### Task 5：补齐回归测试（已完成）

更新并保留以下已有测试语义：

- `clones_share_range_read_permits`：两个限制均跨 clone 共享。
- `configured_range_read_concurrency_can_exceed_32`：真实 peak I/O 仍能达到配置值。
- `range_reads_refill_before_slowest_batch_member_finishes`：释放 I/O 后及时补位。
- `completed_range_buffers_are_released_before_slowest_read`：消费完成的 payload 及时 drop。
- `cloned_reader_is_not_queued_behind_an_entire_batch`：多个 reader 仍共享且公平竞争 I/O。
- `failed_range_read_releases_permit`：改为同时验证 I/O 与 response permit，后续读取不死锁。
- `range_io_stats_count_coalesced_reads`：`peak_in_flight_reads` 仍统计真实 read，而不是 response 生命周期。

运行：

```bash
cargo fmt --all --check
cargo test -p paimon vindex::range_reader
cargo clippy -p paimon --all-targets -- -D warnings
git diff --check
```

本机 Apple ARM 若仍被 `paimon-vindex-core 0.3.0` 的 unstable NEON 编译问题阻塞，至少本地完成 `fmt` 和 `diff --check`，完整 test/clippy 以 Linux GitHub CI 为门禁；不得把依赖绕过改动混入本 PR。

2026-08-18 本地验证记录（Rust `1.97.0`）：

- `cargo fmt --all -- --check` 通过；
- `cargo test -p paimon vindex::range_reader::tests --lib`：20 passed；
- `cargo clippy -p paimon --all-targets -- -D warnings` 通过；
- `git diff --check` 通过。

### Task 6：复跑现有 A/B（待执行）

用同一 benchmark 配置分别比较基线 `1e172b3` 与候选提交，每组至少 3 次取中位数：

1. 无 local cache、无 warmup：确认冷读 QPS/平均延迟变化在约 3% 测试噪声内，Recall/NDCG 完全一致。
2. 4 GiB memory cache、热读：候选版本至少不能比 `1e172b3` 更慢；目标是收回当前相对 `0946fcd` 约 3.4% 的 QPS 差距。
3. 记录 peak RSS、`io_wait`、`range_permit_wait` 和 `peak_in_flight_reads`。RSS 可以高于严格 `C` 响应上限的 `1e172b3`，但不能回到保留全部 range 的增长模式；全局 `2C` 上限由确定性单测负责证明。

如果热 cache 仍稳定回退超过 3%，先保留双 limiter 的正确性改动但不宣称性能改善，使用现有 timing 数据确认瓶颈后再决定是否调整窗口；本计划不预先增加自适应窗口或 byte-based limiter。

## 5. 完成标准

- 真实底层 read 的 peak 不超过 `C`，响应 payload 数全局不超过 `min(2C, MAX_PERMITS)`。
- 同步复制不再持有 I/O permit，clone/file 之间共享两种预算。
- 错误、取消和 receiver 关闭不会泄漏 permit 或挂住后续读取。
- Recall/NDCG 不变，冷读无稳定回退，热 cache 回退被消除或有 timing 证据解释。
- PR 只新增上述两文件的实现/测试改动，不引入新配置、依赖或相邻重构。
