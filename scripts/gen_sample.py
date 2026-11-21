#!/usr/bin/env python3
"""生成乱序的交换机端口事件样例 logs/port_events_shuffled.ndjson（确定性，可重复生成）。"""
import json
import random
from datetime import datetime, timezone
from pathlib import Path

BASE_MS = int(datetime(2026, 11, 21, 8, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)


def main() -> None:
    rng = random.Random(20261121)
    switches = ["sw-core-01", "sw-core-02", "sw-access-01", "sw-access-02"]
    events = []
    for _ in range(64):
        events.append({
            "ts_ms": BASE_MS + rng.randrange(0, 600_000),
            "switch": rng.choice(switches),
            "port": f"Eth1/{rng.randrange(1, 49)}",
            "event": rng.choice(
                ["link-up", "link-down", "flap", "speed-change", "duplex-mismatch"]
            ),
            "severity": rng.choice(["info", "warning", "critical"]),
        })
    # 同一毫秒的多行事件（验证同刻保持原行序）
    tie_ms = BASE_MS + 123_456
    for i, sw in enumerate(switches):
        events.append({
            "ts_ms": tie_ms,
            "switch": sw,
            "port": "Eth1/1",
            "event": "link-down",
            "severity": "critical",
            "burst": i,
        })
    rng.shuffle(events)  # 乱序

    out = Path(__file__).resolve().parent.parent / "logs" / "port_events_shuffled.ndjson"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
    print(f"已生成 {out}（{len(events)} 行）")


if __name__ == "__main__":
    main()
