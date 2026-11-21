"""FastAPI 入口：在 /docs 选择只读目录中的 NDJSON 日志，提交排序、查状态、下载结果。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field

from .config import Settings
from .jobs import (
    InputTooLarge,
    InvalidFilename,
    JobManager,
    JobState,
    LogNotFound,
)


def _iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


class SubmitRequest(BaseModel):
    filename: str = Field(
        ...,
        description="只读日志目录中的 NDJSON 文件名",
        examples=["port_events_shuffled.ndjson"],
    )


def _job_view(snap: dict) -> dict:
    return {
        "job_id": snap["id"],
        "filename": snap["filename"],
        "state": snap["state"].value,
        "error": snap["error"],
        "input_bytes": snap["input_bytes"],
        "reservation_bytes": snap["reservation_bytes"],
        "temp_bytes": snap["temp_bytes"],
        "result_bytes": snap["result_bytes"],
        "shard_count": snap["shard_count"],
        "queue_position": snap["queue_position"],
        "created_at": _iso(snap["created_at"]),
        "started_at": _iso(snap["started_at"]),
        "finished_at": _iso(snap["finished_at"]),
    }


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings.from_env()
    manager = JobManager(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        manager.shutdown()

    app = FastAPI(
        title="交换机端口事件外排服务",
        version="1.0.0",
        description=(
            "交换机故障复盘：按 UTC 毫秒升序回放端口事件。"
            "流程：GET /api/logs 选文件 → POST /api/jobs 提交 → "
            "GET /api/jobs/{job_id} 查状态 → GET /api/jobs/{job_id}/download 下载完整结果。"
        ),
        lifespan=lifespan,
    )
    app.state.manager = manager
    app.state.settings = settings

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/docs")

    @app.get("/api/logs", summary="列出只读目录中的 NDJSON 日志")
    def list_logs():
        return {"logs_dir": str(settings.logs_dir), "logs": manager.list_logs()}

    @app.post("/api/jobs", status_code=202, summary="提交排序作业（按配额并行或排队）")
    def submit(req: SubmitRequest):
        try:
            job = manager.submit(req.filename)
        except LogNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (InvalidFilename, InputTooLarge) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _job_view(manager.snapshot(job.id))

    @app.get("/api/jobs", summary="列出全部作业")
    def list_jobs():
        return {"jobs": [_job_view(s) for s in manager.list_snapshots()]}

    @app.get("/api/jobs/{job_id}", summary="查询作业状态")
    def get_job(job_id: str):
        snap = manager.snapshot(job_id)
        if snap is None:
            raise HTTPException(status_code=404, detail=f"作业不存在：{job_id}")
        return _job_view(snap)

    @app.get(
        "/api/jobs/{job_id}/download",
        summary="下载完整排序结果（仅作业完成后开放）",
    )
    def download(job_id: str):
        snap = manager.snapshot(job_id)
        if snap is None:
            raise HTTPException(status_code=404, detail=f"作业不存在：{job_id}")
        if snap["state"] != JobState.COMPLETED:
            raise HTTPException(
                status_code=409,
                detail=f"作业当前状态为 {snap['state'].value}，仅完整结果可下载",
            )
        path = manager.result_path(job_id)
        if not path.is_file():
            raise HTTPException(status_code=500, detail="结果文件缺失，请重新提交")
        stem = Path(snap["filename"]).stem
        return FileResponse(
            path,
            media_type="application/x-ndjson",
            filename=f"{stem}.sorted.ndjson",
        )

    @app.get("/api/quota", summary="临时盘配额使用情况")
    def quota():
        return manager.quota_status()

    return app


app = create_app()
