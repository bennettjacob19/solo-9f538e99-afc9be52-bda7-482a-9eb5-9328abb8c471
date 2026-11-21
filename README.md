# 交换机端口事件外排服务

交换机故障复盘需要按时间回放端口事件。本服务是**单实例 FastAPI 外排服务**：
在 `/docs` 中从只读目录选择 NDJSON 日志，提交排序、查询状态、下载完整结果。
日志按批排序落盘、逐行多路归并，按 **UTC 毫秒升序**，同一毫秒保持原始行序，每行原样输出。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

`./logs` 以只读方式挂载到容器 `/data/logs`，把 NDJSON 文件放进去即可被选择。
仓库自带乱序样例 `logs/port_events_shuffled.ndjson`（68 行，含同刻事件组）。

### 本地运行

```bash
pip install -r requirements-dev.txt
LOGS_DIR=./logs TEMP_DIR=./data/tmp RESULTS_DIR=./data/results \
  uvicorn app.main:app --port 8000
```

### 使用流程（/docs 或 curl）

```bash
curl localhost:8000/api/logs                                   # 1. 列出只读目录中的日志
curl -X POST localhost:8000/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"filename":"port_events_shuffled.ndjson"}'              # 2. 提交排序 → 202 + job_id
curl localhost:8000/api/jobs/<job_id>                          # 3. 查询状态
curl -OJ localhost:8000/api/jobs/<job_id>/download             # 4. 下载完整结果
curl localhost:8000/api/quota                                  # 配额使用情况
```

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/logs` | 列出只读目录中的 `*.ndjson`（名称 + 字节数） |
| POST | `/api/jobs` | 提交排序作业；202 受理 / 400 输入超配额一半或文件名非法 / 404 文件不存在 |
| GET | `/api/jobs` | 列出全部作业 |
| GET | `/api/jobs/{job_id}` | 作业状态：`queued` / `running` / `completed` / `failed`（含原因） |
| GET | `/api/jobs/{job_id}/download` | 下载完整结果；未完成返回 409，**只开放完整结果** |
| GET | `/api/quota` | 配额、预留、临时区实际字节、运行/排队数 |

## 排序语义

- **键**：每行 JSON 的时间戳字段（默认 `ts_ms`）解析为 UTC 毫秒，升序。
  支持 epoch 毫秒（数字/数字字符串）与 ISO 8601 字符串（`Z`/偏移；朴素时间按 UTC）。
- **稳定**：同一毫秒保持输入文件中的原始行序（批内按行号稳定排序，归并时按分片序号决胜）。
- **原样**：每行字节原样输出。两个例外：纯空白行跳过；文件末行若缺换行符则补一个
  （否则归并后会与下一行粘连）。
- **外排**：按 `BATCH_BYTES` 分批读入、排序、落盘为分片，再逐行 k 路归并到输出，
  内存占用与输入大小无关。

## 配额与调度模型

- **记账**：临时区中所有作业的分片与未完成输出，按文件字节数实时合计
  （`/api/quota` 的 `temp_bytes_used`）。
- **拒绝**：输入字节数 > 配额一半 → 400，不受理。
- **预留**：每个作业预留 `2 × (输入 + 1)` 字节（分片 ≈ 1 倍输入 + 未完成输出 ≈ 1 倍输入，
  归并末期二者同时存在）。
- **并行**：预留总额不超过配额即放行。配额能容纳两项作业各自 2 倍输入时，两项并行。
- **排队**：单项预留够用、合跑不够时，后来的作业 FIFO 排队；前项完成释放额度后自动放行，
  两项都保证完成。
- **失败**：读写/解析失败 → 作业置为 `failed` 并附原因（含行号/errno），
  删除本作业全部分片与未完成输出、释放预留额度；终态在清理完成后才对外可见。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `LOGS_DIR` | `/data/logs` | 只读日志目录 |
| `TEMP_DIR` | `/data/tmp` | 临时区（分片 + 未完成输出，计入配额） |
| `RESULTS_DIR` | `/data/results` | 完整结果目录（不计入配额） |
| `QUOTA_BYTES` | `268435456` | 临时盘配额（字节） |
| `BATCH_BYTES` | `8388608` | 单批排序字节数 |
| `MAX_WORKERS` | `4` | 最大并行作业数 |
| `TS_FIELD` | `ts_ms` | 时间戳字段名 |

`TEMP_DIR` 与 `RESULTS_DIR` 需在同一文件系统（结果用 `os.replace` 原子移出临时区）；
compose 默认布局已满足。

## 自测

```bash
pip install -r requirements-dev.txt
python -m pytest
```

覆盖：排序正确性/稳定性/逐字节原样、超配额一半拒绝、**并行**（屏障证明两作业同时在排序）、
**排队**（合跑差 1 字节时第二项排队且两项都完成）、**写失败清理**（ENOSPC → 原因 + 临时文件删除
+ 额度释放 + 服务恢复）、读失败（输入被并发删除）、非法时间戳带行号报错、下载门禁（未完成 409）。

## 已知限制

- 单实例：作业状态在进程内存中，重启后作业记录丢失（结果文件保留）；多副本部署会使配额记账失效。
- 末行补换行的极端情况下，临时区实际占用最多超出预留 2 字节（空场兜底放行，不死等）。
- 时间戳缺失/非法的行会导致整个作业失败（带行号报错），不做容错跳过。
- 结果文件需重新排序时请重新提交作业；服务不删除已完成的旧结果。
