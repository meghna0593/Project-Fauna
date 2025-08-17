from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, List, Iterable, Optional
import re

RETRY_STATUSES = {500, 502, 503, 504}
ISO_UTC_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")

def chunked(seq: List[Any], size: int) -> Iterable[List[Any]]:
    """Yield successive chunks from seq of length <= size."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def split_friends(s: Optional[str]) -> List[str]:
    """Split a comma-delimited string into a trimmed list; tolerates None/empty."""
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]

def epoch_to_iso8601_utc(epoch: Optional[int | float]) -> Optional[str]:
    """
    Convert epoch to ISO8601 UTC with 'Z'.
    Auto-detect unit (s/ms/µs/ns) by magnitude.
    Returns None for invalid, negative, or future timestamps.
    """
    # Reject negatives and null
    if epoch is None or epoch < 0:
        return None
    
    e = int(epoch)
    now = datetime.now(tz=timezone.utc)

    # Detect units by magnitude
    if e >= 10**18:          # nanoseconds
        ts = e / 1_000_000_000.0
    elif e >= 10**15:        # microseconds
        ts = e / 1_000_000.0
    elif e >= 10**12:        # milliseconds
        ts = e / 1_000.0
    else:                    # seconds
        ts = float(e)
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    
    # Guardrail: keep only dates until present
    return dt.isoformat().replace("+00:00", "Z") if dt <= now else None

def validate_iso8601_utc(z: Optional[str]) -> bool:
    """True iff string is ISO8601 UTC with 'Z' suffix (None allowed)."""
    if z is None:
        return True
    return bool(ISO_UTC_Z_RE.match(z))
