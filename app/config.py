"""服务配置：全部可通过环境变量覆盖。"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"环境变量 {name}={raw!r} 不是合法整数") from exc


@dataclass(frozen=True)
class Settings:
    logs_dir: Path        # 只读日志目录（NDJSON 输入）
    temp_dir: Path        # 临时区：分片 + 未完成输出（按文件字节计入配额）
    results_dir: Path     # 完整结果目录（移出临时区，不再计配额）
    quota_bytes: int      # 临时盘配额（逻辑预算）
    batch_bytes: int      # 单批排序的字节数上限
    max_workers: int      # 最大并行排序线程数
    ts_field: str         # 时间戳字段名

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            logs_dir=Path(os.environ.get("LOGS_DIR", "/data/logs")),
            temp_dir=Path(os.environ.get("TEMP_DIR", "/data/tmp")),
            results_dir=Path(os.environ.get("RESULTS_DIR", "/data/results")),
            quota_bytes=_int_env("QUOTA_BYTES", 256 * 1024 * 1024),
            batch_bytes=_int_env("BATCH_BYTES", 8 * 1024 * 1024),
            max_workers=_int_env("MAX_WORKERS", 4),
            ts_field=os.environ.get("TS_FIELD", "ts_ms"),
        )
