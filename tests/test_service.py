"""端到端自测：排序正确性/稳定性、配额拒绝、并行、排队、失败清理、下载门禁。"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import jobs as jobs_module
from app.config import Settings
from app.main import create_app
from app.sorter import SortError, external_sort, parse_ts_ms

BASE = 1_795_000_000_000  # 约 2026-11 的 epoch 毫秒


# ---------- 工具 ----------

def make_settings(tmp_path: Path, quota_bytes: int, batch_bytes: int = 128,
                  max_workers: int = 4) -> Settings:
    logs = tmp_path / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    return Settings(
        logs_dir=logs,
        temp_dir=tmp_path / "tmp",
        results_dir=tmp_path / "results",
        quota_bytes=quota_bytes,
        batch_bytes=batch_bytes,
        max_workers=max_workers,
        ts_field="ts_ms",
    )


def write_log(logs_dir: Path, name: str, events: list,
              trailing_newline: bool = True, blank_line: bool = False) -> list:
    """写入 NDJSON，返回有效事件行（含换行）的列表。"""
    event_lines = [json.dumps(e, separators=(",", ":")) + "\n" for e in events]
    file_lines = list(event_lines)
    if blank_line:
        file_lines.insert(len(file_lines) // 2, "\n")
    text = "".join(file_lines)
    if not trailing_newline and text.endswith("\n"):
        text = text[:-1]
    (logs_dir / name).write_text(text, encoding="utf-8")
    return event_lines


def expected_body(event_lines: list) -> str:
    """稳定排序后的期望输出（Python sorted 本身稳定；键与服务端解析一致）。"""
    return "".join(sorted(
        event_lines, key=lambda l: parse_ts_ms(json.loads(l)["ts_ms"], 0, "ts_ms")
    ))


def wait_job(client: TestClient, job_id: str, timeout: float = 20.0) -> dict:
    deadline = time.monotonic() + timeout
    snap = {}
    while time.monotonic() < deadline:
        r = client.get(f"/api/jobs/{job_id}")
        assert r.status_code == 200
        snap = r.json()
        if snap["state"] in ("completed", "failed"):
            return snap
        time.sleep(0.02)
    raise AssertionError(f"作业 {job_id} 未在 {timeout}s 内结束，最后状态：{snap}")


def wait_until(pred, timeout: float = 10.0, desc: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(0.02)
    raise AssertionError(f"等待超时：{desc}")


def submit(client: TestClient, filename: str) -> str:
    r = client.post("/api/jobs", json={"filename": filename})
    assert r.status_code == 202, r.text
    return r.json()["job_id"]


def two_log_files(logs_dir: Path):
    """写两个大小相近的日志文件，返回 (文件名列表, 字节数列表)。"""
    logs_dir.mkdir(parents=True, exist_ok=True)
    names, sizes = [], []
    for idx in range(2):
        events = [
            {"ts_ms": BASE + ((i * 37 + idx * 13) % 500), "job": idx, "i": i}
            for i in range(120)
        ]
        write_log(logs_dir, f"j{idx}.ndjson", events)
        names.append(f"j{idx}.ndjson")
        sizes.append((logs_dir / f"j{idx}.ndjson").stat().st_size)
    return names, sizes


# ---------- 正确性 ----------

def test_sort_correctness_stable_and_verbatim(tmp_path):
    settings = make_settings(tmp_path, quota_bytes=1 << 20, batch_bytes=96)
    events = [
        {"ts_ms": BASE + 5000, "switch": "sw1", "port": "Eth1/1", "event": "down"},
        {"ts_ms": BASE + 1000, "switch": "sw1", "port": "Eth1/2", "event": "up"},
        {"ts_ms": BASE + 3000, "switch": "sw2", "port": "Eth1/9", "event": "flap", "seq": 0},
        {"ts_ms": BASE + 3000, "switch": "sw2", "port": "Eth1/10", "event": "flap", "seq": 1},
        {"ts_ms": BASE + 3000, "switch": "sw3", "port": "Eth1/3", "event": "up", "seq": 2},
        {"ts_ms": BASE + 2000, "switch": "sw4", "port": "Eth1/5", "event": "down"},
        {"ts_ms": str(BASE + 1500), "switch": "sw4", "port": "Eth1/6", "event": "up"},
        {"ts_ms": BASE + 4000, "switch": "sw1", "port": "Eth1/1", "event": "up"},
    ]
    # 末行无换行 + 中间夹空白行，输出仍应逐行规整
    event_lines = write_log(settings.logs_dir, "a.ndjson", events,
                            trailing_newline=False, blank_line=True)
    with TestClient(create_app(settings)) as client:
        listed = client.get("/api/logs").json()["logs"]
        assert [e["name"] for e in listed] == ["a.ndjson"]
        assert listed[0]["size_bytes"] == (settings.logs_dir / "a.ndjson").stat().st_size

        job_id = submit(client, "a.ndjson")
        snap = wait_job(client, job_id)
        assert snap["state"] == "completed", snap["error"]
        assert snap["shard_count"] >= 2  # 小批次 → 多个分片 → 走了归并

        r = client.get(f"/api/jobs/{job_id}/download")
        assert r.status_code == 200
        assert "a.sorted.ndjson" in r.headers["content-disposition"]
        body = r.text

        quota = client.get("/api/quota").json()
        assert quota["reserved_bytes"] == 0
        assert quota["temp_bytes_used"] == 0

    assert body == expected_body(event_lines)  # 每行原样、整体有序、同刻稳定
    keys = [parse_ts_ms(json.loads(l)["ts_ms"], 0, "ts_ms") for l in body.splitlines()]
    assert keys == sorted(keys)
    ties = [json.loads(l)["seq"] for l in body.splitlines()
            if json.loads(l)["ts_ms"] == BASE + 3000]
    assert ties == [0, 1, 2]  # 同一毫秒保持原行序
    assert list(settings.temp_dir.iterdir()) == []  # 临时区已清空


# ---------- 受理规则 ----------

def test_reject_input_over_half_quota(tmp_path):
    settings = make_settings(tmp_path, quota_bytes=1000)
    big = [{"ts_ms": BASE + i, "pad": "x" * 64} for i in range(6)]
    write_log(settings.logs_dir, "big.ndjson", big)
    write_log(settings.logs_dir, "small.ndjson", [{"ts_ms": BASE, "pad": "x" * 32}])
    big_size = (settings.logs_dir / "big.ndjson").stat().st_size
    assert big_size > 500  # 超过配额一半

    with TestClient(create_app(settings)) as client:
        r = client.post("/api/jobs", json={"filename": "big.ndjson"})
        assert r.status_code == 400
        assert "配额一半" in r.json()["detail"]

        job_id = submit(client, "small.ndjson")  # 未超一半 → 受理并完成
        assert wait_job(client, job_id)["state"] == "completed"

        assert client.post("/api/jobs", json={"filename": "nope.ndjson"}).status_code == 404
        assert client.post("/api/jobs", json={"filename": "../x.ndjson"}).status_code == 400


# ---------- 并行与排队 ----------

def test_parallel_when_quota_fits_two_jobs(tmp_path, monkeypatch):
    names, sizes = two_log_files(tmp_path / "logs")
    reservations = [2 * (s + 1) for s in sizes]
    quota = sum(reservations)  # 恰好容纳两项作业各自 2 倍输入 → 必须并行
    settings = make_settings(tmp_path, quota_bytes=quota, batch_bytes=256)

    real_sort = jobs_module.external_sort
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    inside = {"cur": 0, "max": 0}

    def gated_sort(*args, **kwargs):
        barrier.wait(timeout=15)  # 两个作业须同时进入排序，否则屏障超时
        with lock:
            inside["cur"] += 1
            inside["max"] = max(inside["max"], inside["cur"])
        try:
            return real_sort(*args, **kwargs)
        finally:
            with lock:
                inside["cur"] -= 1

    monkeypatch.setattr(jobs_module, "external_sort", gated_sort)
    with TestClient(create_app(settings)) as client:
        ids = [submit(client, n) for n in names]
        snaps = [wait_job(client, jid) for jid in ids]
        assert all(s["state"] == "completed" for s in snaps), snaps
        assert inside["max"] == 2  # 两个作业确实同时在排序
        for jid, name in zip(ids, names):
            body = client.get(f"/api/jobs/{jid}/download").text
            lines = (settings.logs_dir / name).read_text().splitlines(keepends=True)
            assert body == expected_body(lines)


def test_queueing_when_quota_fits_only_one(tmp_path, monkeypatch):
    names, sizes = two_log_files(tmp_path / "logs")
    reservations = [2 * (s + 1) for s in sizes]
    quota = sum(reservations) - 1  # 单项够用，合跑差 1 字节 → 第二个排队
    settings = make_settings(tmp_path, quota_bytes=quota, batch_bytes=256)

    real_sort = jobs_module.external_sort
    first_entered = threading.Event()
    release_first = threading.Event()

    def gated_sort(*args, **kwargs):
        if not first_entered.is_set():
            first_entered.set()
            if not release_first.wait(timeout=15):
                raise RuntimeError("测试未及时放行第一个作业")
        return real_sort(*args, **kwargs)

    monkeypatch.setattr(jobs_module, "external_sort", gated_sort)
    with TestClient(create_app(settings)) as client:
        id1 = submit(client, names[0])
        id2 = submit(client, names[1])

        wait_until(first_entered.is_set, desc="第一个作业进入排序")
        s1 = client.get(f"/api/jobs/{id1}").json()
        s2 = client.get(f"/api/jobs/{id2}").json()
        assert s1["state"] == "running"
        assert s2["state"] == "queued"  # 合跑不够 → 排队
        assert s2["queue_position"] == 0
        quota_now = client.get("/api/quota").json()
        assert quota_now["running_jobs"] == 1
        assert quota_now["queued_jobs"] == 1

        release_first.set()
        snaps = [wait_job(client, jid) for jid in (id1, id2)]
        assert all(s["state"] == "completed" for s in snaps), snaps  # 两项都须完成

        for jid, name in zip((id1, id2), names):
            body = client.get(f"/api/jobs/{jid}/download").text
            lines = (settings.logs_dir / name).read_text().splitlines(keepends=True)
            assert body == expected_body(lines)
        quota_end = client.get("/api/quota").json()
        assert quota_end["reserved_bytes"] == 0
        assert quota_end["temp_bytes_used"] == 0


# ---------- 失败清理 ----------

def test_write_failure_cleans_temp_and_releases_quota(tmp_path, monkeypatch):
    settings = make_settings(tmp_path, quota_bytes=1 << 20)
    write_log(settings.logs_dir, "x.ndjson",
              [{"ts_ms": BASE + i, "pad": "y" * 40} for i in range(20)])
    real_sort = jobs_module.external_sort

    def boom_sort(src, work_dir, partial_out, **kwargs):
        # 模拟写盘失败：分片写了一半、输出写了一半就报 ENOSPC
        work_dir = Path(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        (work_dir / "shard-00000.ndjson").write_bytes(b"z" * 256)
        Path(partial_out).write_bytes(b"partial-output")
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(jobs_module, "external_sort", boom_sort)
    with TestClient(create_app(settings)) as client:
        job_id = submit(client, "x.ndjson")
        snap = wait_job(client, job_id)
        assert snap["state"] == "failed"
        assert "No space left on device" in snap["error"]  # 说明原因
        assert list(settings.temp_dir.iterdir()) == []      # 本作业临时文件已删除
        quota = client.get("/api/quota").json()
        assert quota["reserved_bytes"] == 0                 # 额度已释放
        assert quota["temp_bytes_used"] == 0

        # 服务恢复健康：后续作业照常完成
        monkeypatch.setattr(jobs_module, "external_sort", real_sort)
        job_id2 = submit(client, "x.ndjson")
        assert wait_job(client, job_id2)["state"] == "completed"


def test_read_failure_reports_reason_and_cleans_up(tmp_path, monkeypatch):
    settings = make_settings(tmp_path, quota_bytes=1 << 20)
    write_log(settings.logs_dir, "gone.ndjson", [{"ts_ms": BASE, "a": 1}])
    real_sort = jobs_module.external_sort

    def deleting_sort(src, *args, **kwargs):
        Path(src).unlink()  # 模拟输入文件在排序前被并发删除
        return real_sort(src, *args, **kwargs)

    monkeypatch.setattr(jobs_module, "external_sort", deleting_sort)
    with TestClient(create_app(settings)) as client:
        job_id = submit(client, "gone.ndjson")
        snap = wait_job(client, job_id)
        assert snap["state"] == "failed"
        assert "读取输入文件失败" in snap["error"]  # 说明原因
        assert list(settings.temp_dir.iterdir()) == []
        quota = client.get("/api/quota").json()
        assert quota["reserved_bytes"] == 0
        assert quota["temp_bytes_used"] == 0


def test_invalid_timestamp_fails_with_line_number(tmp_path):
    settings = make_settings(tmp_path, quota_bytes=1 << 20)
    (settings.logs_dir / "bad.ndjson").write_text(
        '{"ts_ms": 1000}\n{"ts_ms": "not-a-time"}\n', encoding="utf-8"
    )
    with TestClient(create_app(settings)) as client:
        job_id = submit(client, "bad.ndjson")
        snap = wait_job(client, job_id)
        assert snap["state"] == "failed"
        assert "第 2 行" in snap["error"]
        assert list(settings.temp_dir.iterdir()) == []


# ---------- 下载门禁 ----------

def test_download_only_when_completed(tmp_path, monkeypatch):
    settings = make_settings(tmp_path, quota_bytes=1 << 20)
    event_lines = write_log(settings.logs_dir, "d.ndjson",
                            [{"ts_ms": BASE + 2, "a": 1}, {"ts_ms": BASE + 1, "a": 2}])
    real_sort = jobs_module.external_sort
    entered = threading.Event()
    release = threading.Event()

    def gated_sort(*args, **kwargs):
        entered.set()
        release.wait(timeout=15)
        return real_sort(*args, **kwargs)

    monkeypatch.setattr(jobs_module, "external_sort", gated_sort)
    with TestClient(create_app(settings)) as client:
        job_id = submit(client, "d.ndjson")
        wait_until(entered.is_set, desc="作业进入排序")

        r = client.get(f"/api/jobs/{job_id}/download")  # 进行中 → 拒绝
        assert r.status_code == 409
        assert "仅完整结果可下载" in r.json()["detail"]
        assert client.get("/api/jobs/unknown/download").status_code == 404
        assert client.get("/api/jobs/unknown").status_code == 404

        release.set()
        assert wait_job(client, job_id)["state"] == "completed"
        r = client.get(f"/api/jobs/{job_id}/download")  # 完成后 → 放行
        assert r.status_code == 200
        assert r.text == expected_body(event_lines)


# ---------- 排序器单元测试 ----------

def test_external_sort_unit_shards_and_cleanup(tmp_path):
    src = tmp_path / "in.ndjson"
    events = [{"ts_ms": (i * 7919) % 1000, "i": i} for i in range(500)]
    lines = [json.dumps(e) + "\n" for e in events]
    src.write_text("".join(lines), encoding="utf-8")
    work, out = tmp_path / "work", tmp_path / "out.ndjson"

    stats = external_sort(src, work, out, batch_bytes=200, ts_field="ts_ms")

    assert stats.shard_count > 1
    assert out.read_text() == "".join(
        sorted(lines, key=lambda l: json.loads(l)["ts_ms"])
    )
    assert stats.output_bytes == len(out.read_bytes())
    assert list(work.iterdir()) == []  # 分片已清理


def test_external_sort_missing_input(tmp_path):
    with pytest.raises(SortError, match="读取输入文件失败"):
        external_sort(tmp_path / "nope.ndjson", tmp_path / "w", tmp_path / "o",
                      batch_bytes=100, ts_field="ts_ms")


def test_parse_ts_ms_formats():
    assert parse_ts_ms(1795000000000, 1, "ts_ms") == 1795000000000
    assert parse_ts_ms("1795000000000", 1, "ts_ms") == 1795000000000
    zulu = parse_ts_ms("2026-11-21T08:00:00Z", 1, "ts_ms")
    assert zulu == parse_ts_ms("2026-11-21T08:00:00+00:00", 1, "ts_ms")
    assert parse_ts_ms("2026-11-21T10:00:00+02:00", 1, "ts_ms") == zulu
    assert parse_ts_ms("2026-11-21 08:00:00", 1, "ts_ms") == zulu  # 朴素时间按 UTC
    with pytest.raises(SortError):
        parse_ts_ms("not-a-time", 1, "ts_ms")
    with pytest.raises(SortError):
        parse_ts_ms(True, 1, "ts_ms")
