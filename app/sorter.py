"""单机外排：分批排序落盘 + 逐行多路归并。

排序语义：
- 键：每行 JSON 中时间戳字段解析出的 UTC 毫秒，升序；
- 同一时间戳保持输入文件中的原始行序（稳定排序）；
- 每行字节原样输出（仅对文件末行补一个换行符，避免归并时与下一行粘连；
  纯空白行跳过）。

磁盘占用上界：分片合计 ≈ 输入字节数，输出 ≈ 输入字节数，
归并末期二者同时存在，峰值 ≈ 2 倍输入。
"""
from __future__ import annotations

import heapq
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

# 临时区字节变动回调：正数 = 新增字节，负数 = 移除字节
BytesCallback = Callable[[int], None]


class SortError(Exception):
    """可解释的排序失败（读写/解析），消息面向用户。"""


@dataclass
class SortStats:
    input_bytes: int
    output_bytes: int
    shard_count: int


def parse_ts_ms(value: object, lineno: int, field: str) -> int:
    """把时间戳字段值解析为 UTC 毫秒。

    支持：epoch 毫秒（数字或数字字符串）、ISO 8601 字符串（可带 Z/偏移，
    朴素时间按 UTC 处理）。解析失败抛出带行号的 SortError。
    """
    if isinstance(value, bool):
        raise SortError(f"第 {lineno} 行：字段 {field!r} 的布尔值不能作为时间戳")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        try:
            return int(text)
        except ValueError:
            pass
        iso = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
        try:
            dt = datetime.fromisoformat(iso)
        except ValueError:
            raise SortError(
                f"第 {lineno} 行：字段 {field!r} 的值 {value!r} 既不是 epoch 毫秒也不是 ISO 8601 时间"
            ) from None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    raise SortError(
        f"第 {lineno} 行：字段 {field!r} 的类型 {type(value).__name__} 无法解析为时间戳"
    )


def line_key(line: bytes, field: str, lineno: int) -> int:
    """提取一行的排序键（UTC 毫秒）。失败抛出带行号的 SortError。"""
    try:
        obj = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SortError(f"第 {lineno} 行：不是合法 JSON（{exc}）") from exc
    if not isinstance(obj, dict):
        raise SortError(f"第 {lineno} 行：JSON 不是对象，无法读取字段 {field!r}")
    if field not in obj:
        raise SortError(f"第 {lineno} 行：缺少时间戳字段 {field!r}")
    return parse_ts_ms(obj[field], lineno, field)


def _flush(batch: list, work_dir: Path, index: int, on_bytes: Optional[BytesCallback]) -> Path:
    """把一批 (key, seq, line) 稳定排序后落盘为一个分片。"""
    batch.sort(key=lambda item: (item[0], item[1]))
    path = work_dir / f"shard-{index:05d}.ndjson"
    written = 0
    try:
        with open(path, "wb") as fh:
            for _, _, line in batch:
                fh.write(line)
                written += len(line)
    except OSError as exc:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        raise SortError(f"写入分片文件失败：{exc}（{path}）") from exc
    if on_bytes:
        on_bytes(written)
    return path


def _merge(shards: list, out_path: Path, ts_field: str, on_bytes: Optional[BytesCallback]) -> int:
    """逐行 k 路归并。堆项 (key, shard_idx, line)：key 相同取小编号分片，
    而分片编号随输入位置递增，因此同刻保持原始行序。"""
    handles = []
    try:
        for path in shards:
            handles.append(open(path, "rb"))
    except OSError as exc:
        for fh in handles:
            fh.close()
        raise SortError(f"读取分片文件失败：{exc}") from exc

    def _readline(idx: int) -> bytes:
        try:
            return handles[idx].readline()
        except OSError as exc:
            raise SortError(f"读取分片文件失败：{exc}（{shards[idx]}）") from exc

    try:
        heap = []
        for idx in range(len(handles)):
            line = _readline(idx)
            if line:
                heap.append((line_key(line, ts_field, 0), idx, line))
        heapq.heapify(heap)
        written = 0
        try:
            with open(out_path, "wb") as out:
                while heap:
                    _, idx, line = heapq.heappop(heap)
                    out.write(line)
                    written += len(line)
                    nxt = _readline(idx)
                    if nxt:
                        heapq.heappush(heap, (line_key(nxt, ts_field, 0), idx, nxt))
        except OSError as exc:
            raise SortError(f"写入输出文件失败：{exc}（{out_path}）") from exc
    finally:
        for fh in handles:
            fh.close()
    if on_bytes:
        on_bytes(written)
    return written


def external_sort(
    src,
    work_dir,
    partial_out,
    *,
    batch_bytes: int,
    ts_field: str,
    on_bytes: Optional[BytesCallback] = None,
) -> SortStats:
    """把 src（NDJSON）外排到 partial_out。

    分批读入并排序落盘（分片位于 work_dir），再逐行归并到 partial_out；
    成功后删除全部分片。任何读写/解析失败都抛出带原因的 SortError，
    临时文件清理由调用方负责。
    """
    if batch_bytes <= 0:
        raise ValueError("batch_bytes 必须为正数")
    src = Path(src)
    work_dir = Path(work_dir)
    partial_out = Path(partial_out)
    work_dir.mkdir(parents=True, exist_ok=True)

    shards: list[Path] = []
    total_in = 0
    try:
        fin = open(src, "rb")
    except OSError as exc:
        raise SortError(f"读取输入文件失败：{exc}（{src}）") from exc

    with fin:
        batch: list = []
        batch_used = 0
        lineno = 0
        while True:
            try:
                raw = fin.readline()
            except OSError as exc:
                raise SortError(f"读取输入文件失败（第 {lineno + 1} 行附近）：{exc}") from exc
            if not raw:
                break
            lineno += 1
            if not raw.strip():
                continue  # 跳过空白行
            if not raw.endswith(b"\n"):
                raw += b"\n"  # 末行补换行，避免归并后与下一行粘连
            key = line_key(raw, ts_field, lineno)
            batch.append((key, lineno, raw))  # lineno 递增，即原始行序
            batch_used += len(raw)
            total_in += len(raw)
            if batch_used >= batch_bytes:
                shards.append(_flush(batch, work_dir, len(shards), on_bytes))
                batch = []
                batch_used = 0
        if batch:
            shards.append(_flush(batch, work_dir, len(shards), on_bytes))

    output_bytes = _merge(shards, partial_out, ts_field, on_bytes)

    freed = 0
    for shard in shards:
        try:
            freed += shard.stat().st_size
            shard.unlink()
        except OSError:
            pass  # 清理失败不影响结果，由调用方兜底删除
    if on_bytes and freed:
        on_bytes(-freed)

    return SortStats(input_bytes=total_in, output_bytes=output_bytes, shard_count=len(shards))
