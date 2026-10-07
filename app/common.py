"""Shared helpers used by the control service and the printer simulator."""
from __future__ import annotations

import hashlib
import json


def is_printable_ascii(text: str) -> bool:
    """1..80 printable ASCII characters (0x20..0x7E, no tabs/newlines)."""
    if not isinstance(text, str) or not (1 <= len(text) <= 80):
        return False
    return all(0x20 <= ord(ch) <= 0x7E for ch in text)


def op_key(batch_id: str, seq: int) -> str:
    """The immutable operation key sent to the printer: <batch-id>#L<seq>."""
    return f"{batch_id}#L{int(seq)}"


def sorted_labels(labels: list[dict]) -> list[dict]:
    """Labels are always processed in fixed ascending sequence order."""
    return sorted(labels, key=lambda item: item["seq"])


def content_hash(labels: list[dict]) -> str:
    """Stable hash over the full ordered label payload of a batch."""
    canonical = json.dumps(
        [{"seq": item["seq"], "text": item["text"]} for item in sorted_labels(labels)],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()
