"""Pydantic models and validation rules."""

import hashlib
import json

from pydantic import BaseModel, Field

MAX_LABELS = 8
MIN_LEN = 1
MAX_LEN = 80


class LabelIn(BaseModel):
    seq: int = Field(..., ge=1, le=10**9)
    content: str


class BatchIn(BaseModel):
    request_key: str = Field(..., min_length=1, max_length=200)
    labels: list[LabelIn]


def validate_labels(labels):
    """Return list of normalized (seq, content) or raise ValueError(400)."""
    if not labels:
        raise ValueError("空批次整单拒绝：至少提交 1 张标签")
    if len(labels) > MAX_LABELS:
        raise ValueError(f"每批最多 {MAX_LABELS} 张标签")

    seen = set()
    out = []
    for lab in labels:
        c = lab.content
        if len(c) < MIN_LEN or len(c) > MAX_LEN:
            raise ValueError(f"序号 {lab.seq}: 正文长度须为 1~{MAX_LEN} 个字符")
        # 可打印 ASCII：0x20..0x7E
        bad = [ch for ch in c if not (0x20 <= ord(ch) <= 0x7E)]
        if bad:
            raise ValueError(
                f"序号 {lab.seq}: 含不可打印/非 ASCII 字符（首个 U+{ord(bad[0]):04X}）"
            )
        if lab.seq in seen:
            raise ValueError(f"重复序号 {lab.seq}")
        seen.add(lab.seq)
        out.append((lab.seq, c))
    out.sort(key=lambda x: x[0])
    return out


def content_hash(seq_content):
    canonical = json.dumps(
        [{"seq": s, "content": c} for s, c in seq_content],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()
