"""
Compat shim for the removed stdlib module `imghdr` on Python >=3.13.

Provides a minimal `what(file, h=None)` implementation used by libraries
to detect common image types from magic numbers. This avoids import errors
in packages that still rely on `imghdr` (e.g., older Streamlit builds).
"""

from __future__ import annotations
from typing import Optional

# Magic numbers for common image formats
MAGIC = {
    "jpeg": [b"\xFF\xD8\xFF"],
    "png": [b"\x89PNG\r\n\x1a\n"],
    "gif": [b"GIF87a", b"GIF89a"],
    "bmp": [b"BM"],
    "tiff": [b"II*\x00", b"MM\x00*"],
    "webp": [b"RIFF"],  # further check for 'WEBP' at offset 8
}


def _detect(header: bytes) -> Optional[str]:
    if not header:
        return None

    for fmt, sigs in MAGIC.items():
        for sig in sigs:
            if header.startswith(sig):
                if fmt == "webp":
                    # 'RIFF....WEBP' structure
                    if len(header) >= 12 and header[8:12] == b"WEBP":
                        return "webp"
                    continue
                return fmt
    return None


def what(file: Optional[str], h: Optional[bytes] = None) -> Optional[str]:
    """
    Determine the image type of a file or by-provided header bytes.

    Parameters
    - file: Path to the file (str) or None
    - h: Optional bytes-like object containing the file header

    Returns
    - A string indicating the image type (e.g., 'jpeg', 'png'), or None
    """
    if h is not None:
        return _detect(h)

    if file is None:
        return None

    try:
        with open(file, "rb") as f:
            header = f.read(16)
        return _detect(header)
    except Exception:
        return None