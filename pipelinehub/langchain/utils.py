from typing import Optional


def truncate(text: Optional[object], max_len: int = 500) -> str:
    if not text and text != 0:
        return ""
    s = str(text)
    if len(s) <= max_len:
        return s
    return s[:max_len] + "..."
