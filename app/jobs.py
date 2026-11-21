"""作业管理：受理校验、配额预留、排队/并行调度、失败清理与额度释放。

配额模型：
- 记账（实际）：临时区中所有作业的分片与未完成输出的文件字节合计
  （temp_bytes_used，经 on_bytes 回调逐笔增减）。
- 预留（调度）：每个作业按 2*(输入+1) 字节预留额度，覆盖"分片 ≈ 1 倍输入 +
  未完成输出 ≈ 1 倍输入"的峰值；预留总额不超过配额即可并行，否则 FIFO 排队。
- 输入超过配额一半直接拒绝。
"""
from __future__ import annotations

import os
import shutil
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

from .config import Settings
from .sorter import SortError, external_sort


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class LogNotFound(Exception):
    """日志文件不存在或不可读。"""


class InvalidFilename(Exception):
    """文件名非法（为空或越出日志目录）。"""


class InputTooLarge(Exception):
    """输入超过配额一半。"""


@dataclass
class Job:
    id: str
    filename: str
    input_bytes: int
    reservation_bytes: int
    state: JobState = JobState.QUEUED
    error: Optional[str] = None
    temp_bytes: int = 0
    result_bytes: Optional[int] = None
    shard_count: int = 0
    created_at: float = 0.0
    started_at: Optional[float] = None
    finished_at: Optional[float] = None


class JobManager:
    """单实例作业调度器（进程内状态，勿多副本部署）。"""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.settings.temp_dir.mkdir(parents=True, exist_ok=True)
        self.settings.results_dir.mkdir(parents=True, exist_ok=True)
        self._cond = threading.Condition()
        self._jobs: dict[str, Job] = {}
        self._queue: deque[str] = deque()
        self._reserved = 0   # 运行中作业的预留额度合计
        self._temp_used = 0  # 临时区实际文件字节合计（分片 + 未完成输出）
        self._active = 0
        self._executor = ThreadPoolExecutor(
            max_workers=settings.max_workers, thread_name_prefix="sort-worker"
        )

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)

    # ---- 日志目录 ----

    def list_logs(self) -> list[dict]:
        try:
            entries = sorted(self.settings.logs_dir.iterdir())
        except OSError:
            return []
        out = []
        for path in entries:
            try:
                if path.is_file() and path.suffix == ".ndjson":
                    out.append({"name": path.name, "size_bytes": path.stat().st_size})
            except OSError:
                continue
        return out

    def _resolve(self, filename: str) -> Path:
        if not filename or not filename.strip():
            raise InvalidFilename("文件名不能为空")
        base = self.settings.logs_dir.resolve()
        candidate = (base / filename).resolve()
        if not candidate.is_relative_to(base):
            raise InvalidFilename(f"非法文件名 {filename!r}：必须位于日志目录内")
        return candidate

    # ---- 提交与查询 ----

    def submit(self, filename: str) -> Job:
        path = self._resolve(filename)
        try:
            if not path.is_file():
                raise LogNotFound(f"日志不存在：{filename}")
            size = path.stat().st_size
        except LogNotFound:
            raise
        except OSError as exc:
            raise LogNotFound(f"日志不可读：{filename}（{exc}）") from exc
        half = self.settings.quota_bytes // 2
        if size > half:
            raise InputTooLarge(
                f"输入 {size} 字节超过配额一半（{half} 字节），拒绝受理"
            )
        job = Job(
            id=uuid.uuid4().hex,
            filename=filename,
            input_bytes=size,
            reservation_bytes=2 * (size + 1),
            created_at=time.time(),
        )
        with self._cond:
            self._jobs[job.id] = job
            self._queue.append(job.id)
            self._pump_locked()
        return job

    def snapshot(self, job_id: str) -> Optional[dict]:
        with self._cond:
            job = self._jobs.get(job_id)
            return None if job is None else self._snapshot_locked(job)

    def list_snapshots(self) -> list[dict]:
        with self._cond:
            ordered = sorted(self._jobs.values(), key=lambda j: j.created_at)
            return [self._snapshot_locked(j) for j in ordered]

    def _snapshot_locked(self, job: Job) -> dict:
        queue_position = None
        if job.state == JobState.QUEUED:
            try:
                queue_position = list(self._queue).index(job.id)
            except ValueError:
                queue_position = None
        return {
            "id": job.id,
            "filename": job.filename,
            "state": job.state,
            "error": job.error,
            "input_bytes": job.input_bytes,
            "reservation_bytes": job.reservation_bytes,
            "temp_bytes": job.temp_bytes,
            "result_bytes": job.result_bytes,
            "shard_count": job.shard_count,
            "queue_position": queue_position,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
        }

    def quota_status(self) -> dict:
        with self._cond:
            return {
                "quota_bytes": self.settings.quota_bytes,
                "reserved_bytes": self._reserved,
                "temp_bytes_used": self._temp_used,
                "available_bytes": self.settings.quota_bytes - self._reserved,
                "running_jobs": self._active,
                "queued_jobs": len(self._queue),
            }

    def result_path(self, job_id: str) -> Path:
        return self.settings.results_dir / f"{job_id}.ndjson"

    # ---- 调度 ----

    def _pump_locked(self) -> None:
        """按 FIFO 放行队列头部作业：预留额度放得下才并行，否则排队。"""
        while self._queue and self._active < self.settings.max_workers:
            job = self._jobs[self._queue[0]]
            fits = self._reserved + job.reservation_bytes <= self.settings.quota_bytes
            # 兜底：输入 ≤ 配额一半时预留最多超出配额 2 字节（末行补换行），
            # 空场时放行可避免极端配额下死等。
            alone = self._active == 0 and self._reserved == 0
            if not (fits or alone):
                break
            self._queue.popleft()
            self._reserved += job.reservation_bytes
            self._active += 1
            job.state = JobState.RUNNING
            job.started_at = time.time()
            self._executor.submit(self._run, job)
        self._cond.notify_all()

    def _add_temp(self, job_id: str, delta: int) -> None:
        with self._cond:
            job = self._jobs.get(job_id)
            if job is not None:
                job.temp_bytes += delta
                self._temp_used += delta

    def _run(self, job: Job) -> None:
        work_dir = self.settings.temp_dir / job.id
        partial = self.settings.temp_dir / f"{job.id}.out"
        stats = None
        error = None
        try:
            src = self._resolve(job.filename)
            if not src.is_file():
                raise SortError(f"读取输入文件失败：{job.filename} 已不存在")
            stats = external_sort(
                src,
                work_dir,
                partial,
                batch_bytes=self.settings.batch_bytes,
                ts_field=self.settings.ts_field,
                on_bytes=lambda delta: self._add_temp(job.id, delta),
            )
            final = self.result_path(job.id)
            try:
                os.replace(partial, final)
            except OSError as exc:
                raise SortError(f"保存结果失败：{exc}") from exc
            # 完整结果移出临时区，不再占用配额
            self._add_temp(job.id, -stats.output_bytes)
        except Exception as exc:  # 失败必须说明原因
            stats = None
            error = f"{type(exc).__name__}: {exc}"
        # 先删除本作业临时文件、释放额度，再对外呈现终态：
        # 任何观察者看到终态时，清理与额度释放都已生效。
        self._cleanup_temp(job, work_dir, partial)
        with self._cond:
            if error is None:
                job.state = JobState.COMPLETED
                job.result_bytes = stats.output_bytes
                job.shard_count = stats.shard_count
            else:
                job.state = JobState.FAILED
                job.error = error
            self._reserved -= job.reservation_bytes
            self._active -= 1
            job.finished_at = time.time()
            self._pump_locked()

    def _cleanup_temp(self, job: Job, work_dir: Path, partial: Path) -> None:
        shutil.rmtree(work_dir, ignore_errors=True)
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            pass
        with self._cond:
            self._temp_used -= job.temp_bytes
            job.temp_bytes = 0
