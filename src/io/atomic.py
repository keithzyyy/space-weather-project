from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests

# -----------------------------
# Disk writes: manifest/chunks/success
# -----------------------------

def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def write_success(run_dir: Path) -> None:
    (run_dir / "_SUCCESS").write_text("", encoding="utf-8")


def write_failed(run_dir: Path, message: str) -> None:
    (run_dir / "_FAILED").write_text(message, encoding="utf-8")