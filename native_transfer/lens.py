from io import BufferedIOBase
from struct import unpack
from typing import Optional

from .errors import (
    NativeReadError,
    NativeWriteError,
)


def read_lens(file: BufferedIOBase) -> int:
    """Decoding length from ClickHouse Native Format (number of columns, number of rows, length of row)."""

    shift = 0
    x = 0

    for _ in range(10):
        _byte = unpack("<b", file.read(1))[0]
        x |= (_byte & 0x7f) << shift

        if _byte & 0x80 == 0:
            return x

        shift += 7

    raise NativeReadError("Invalid UInt value!")


def write_lens(lens: int, file: Optional[BufferedIOBase] = None) -> bytes:
    """Encoding length into ClickHouse Native Format (number of columns, number of rows, length of row)."""

    x = b""

    for _ in range(10):

        shift = lens & 0x7F
        lens >>= 7

        if lens > 0:
            shift |= 0x80

        x += bytes([shift])

        if lens == 0:

            if file:
                file.write(x)

            return x

    raise NativeWriteError("Invalid UInt value!")
