#!/usr/bin/env python3
"""
File: mock_log_generator.py

Generates mock log data and writes:
- mockData.json (records)
- mockData_ext_map.json (extension id -> extension string)

Record schema:
{
  "ID-0123456789": {
      "ext_id": 3,                        # int
      "file_len": 12,                     # int
      "ext_danger": 0.05,                 # float in [0, 1]
      "success": 1,                       # int {0,1}
      "transfer_start_ms": 1743465600123, # int epoch milliseconds (UTC)
      "transfer_finish_ms": 1743465660456,# int epoch milliseconds (UTC)
      "transfer_delta_s": 60.333          # float seconds
  }
}
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple


NORMAL_EXTENSIONS = [".txt", ".xlsx", ".doc", ".xml", ".zip", ".png", ".pdf"]
OUTLIER_EXTENSIONS = [".ps1", ".cmd", ".rar"]

# 0 (safe) -> 1 (most dangerous). Unknown/edge => 1.0
EXT_DANGER: Dict[str, float] = {
    ".txt": 0.05,
    ".png": 0.05,
    ".pdf": 0.20,
    ".zip": 0.35,
    ".xml": 0.40,
    ".doc": 0.55,
    ".xlsx": 0.55,
    ".rar": 0.60,
    ".cmd": 0.95,
    ".ps1": 1.00,
}


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date


DATE_RANGE_2025_FROM_APRIL = DateRange(start=date(2025, 4, 1), end=date(2025, 12, 31))


def _normalize_ext(ext: str) -> str:
    key = (ext or "").strip().lower()
    if not key:
        return ""
    if not key.startswith("."):
        key = "." + key
    return key


def _build_ext_id_maps() -> Tuple[Dict[str, int], Dict[int, str]]:
    """
    Builds stable extension IDs from all known extensions.
    Unknown extensions map to ext_id=0.
    """
    all_exts = set(map(_normalize_ext, NORMAL_EXTENSIONS + OUTLIER_EXTENSIONS + list(EXT_DANGER.keys())))
    all_exts.discard("")
    sorted_exts = sorted(all_exts)

    ext_to_id: Dict[str, int] = {"": 0, ".unknown": 0}
    id_to_ext: Dict[int, str] = {0: ".unknown"}

    for idx, ext in enumerate(sorted_exts, start=1):
        ext_to_id[ext] = idx
        id_to_ext[idx] = ext

    return ext_to_id, id_to_ext


EXT_TO_ID, ID_TO_EXT = _build_ext_id_maps()


def _random_id(existing: set[str]) -> str:
    while True:
        key = f"ID-{random.randint(0, 9_999_999_999):010d}"
        if key not in existing:
            return key


def _random_date_in_range(dr: DateRange) -> date:
    span_days = (dr.end - dr.start).days
    return dr.start + timedelta(days=random.randint(0, span_days))


def _epoch_ms_utc(dt: datetime) -> int:
    dt_utc = dt.replace(tzinfo=timezone.utc)
    return int(dt_utc.timestamp() * 1000)


def _danger_for_extension(ext: str) -> float:
    key = _normalize_ext(ext)
    score = EXT_DANGER.get(key, 1.0)
    if not isinstance(score, (int, float)):
        return 1.0
    return float(max(0.0, min(1.0, score)))


def _ext_id(ext: str) -> int:
    return int(EXT_TO_ID.get(_normalize_ext(ext), 0))


def _random_extension(pool: list[str]) -> str:
    return random.choice(pool)


def _random_file_length(min_len: int, max_len: int) -> int:
    return random.randint(min_len, max_len)


def _random_start_in_business_hours() -> datetime:
    d = _random_date_in_range(DATE_RANGE_2025_FROM_APRIL)
    sec = random.randint(8 * 3600, 17 * 3600)
    ms = random.randint(0, 999)
    return datetime.combine(d, time(0, 0, 0)) + timedelta(seconds=sec, milliseconds=ms)


def _random_start_finish_in_night_window() -> Tuple[datetime, datetime]:
    """
    Outlier window: start/finish constrained to 23:00-04:00.
    Pick a segment and cap duration so finish stays inside the segment.
    """
    segment = random.choice(["late", "early"])

    if segment == "late":
        d = _random_date_in_range(DATE_RANGE_2025_FROM_APRIL)
        seg_start = datetime.combine(d, time(23, 0, 0))
        seg_end = datetime.combine(d, time(23, 59, 59, 999000))
        start_offset_sec = random.randint(0, 59 * 60 + 59)
        start_ms = random.randint(0, 999)
        start_dt = seg_start + timedelta(seconds=start_offset_sec, milliseconds=start_ms)
    else:
        dr = DateRange(start=date(2025, 4, 2), end=DATE_RANGE_2025_FROM_APRIL.end)
        d = _random_date_in_range(dr)
        seg_start = datetime.combine(d, time(0, 0, 0))
        seg_end = datetime.combine(d, time(4, 0, 0))
        start_offset_sec = random.randint(0, 4 * 3600)
        start_ms = random.randint(0, 999)
        start_dt = seg_start + timedelta(seconds=start_offset_sec, milliseconds=start_ms)

    max_duration_ms = max(1000, int((seg_end - start_dt).total_seconds() * 1000))
    duration_ms = random.randint(1000, min(120_000, max_duration_ms))
    finish_dt = start_dt + timedelta(milliseconds=duration_ms)
    return start_dt, finish_dt


def generate_mock_data(
    normal_count: int = 1000,
    outlier_count: int = 20,
    success_rate: float = 0.99,
) -> Dict[str, dict]:
    data: Dict[str, dict] = {}
    existing_ids: set[str] = set()

    def success_flag() -> int:
        return 1 if random.random() < success_rate else 0

    # Normal entries
    for _ in range(normal_count):
        key = _random_id(existing_ids)
        existing_ids.add(key)

        ext = _random_extension(NORMAL_EXTENSIONS)
        file_len = _random_file_length(1, 25)

        start_dt = _random_start_in_business_hours()
        duration_ms = random.randint(1000, 120_000)
        finish_dt = start_dt + timedelta(milliseconds=duration_ms)

        start_ms = _epoch_ms_utc(start_dt)
        finish_ms = _epoch_ms_utc(finish_dt)
        delta_s = (finish_ms - start_ms) / 1000.0

        data[key] = {
            "ext_id": _ext_id(ext),
            "file_len": int(file_len),
            "ext_danger": float(_danger_for_extension(ext)),
            "success": int(success_flag()),
            "transfer_start_ms": int(start_ms),
            "transfer_finish_ms": int(finish_ms),
            "transfer_delta_s": float(delta_s),
        }

    # Outlier entries (no explicit marker)
    for _ in range(outlier_count):
        key = _random_id(existing_ids)
        existing_ids.add(key)

        ext = _random_extension(OUTLIER_EXTENSIONS)
        file_len = _random_file_length(30, 60)

        start_dt, finish_dt = _random_start_finish_in_night_window()
        start_ms = _epoch_ms_utc(start_dt)
        finish_ms = _epoch_ms_utc(finish_dt)
        delta_s = (finish_ms - start_ms) / 1000.0

        data[key] = {
            "ext_id": _ext_id(ext),
            "file_len": int(file_len),
            "ext_danger": float(_danger_for_extension(ext)),
            "success": int(success_flag()),
            "transfer_start_ms": int(start_ms),
            "transfer_finish_ms": int(finish_ms),
            "transfer_delta_s": float(delta_s),
        }

    return data


def main() -> None:
    data = generate_mock_data(normal_count=1000, outlier_count=20, success_rate=0.99)

    Path("mockData.json").write_text(json.dumps(data, indent=2), encoding="utf-8")
    Path("mockData_ext_map.json").write_text(json.dumps(ID_TO_EXT, indent=2), encoding="utf-8")

    print(f"Wrote {len(data)} records to {Path('mockData.json').resolve()}")
    print(f"Wrote extension map to {Path('mockData_ext_map.json').resolve()}")


if __name__ == "__main__":
    main()
