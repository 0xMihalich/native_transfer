from io import BufferedIOBase
from typing import (
    Optional,
    Union,
)

from ..lens import (
    read_lens,
    write_lens,
)


def read_string(file: BufferedIOBase, lens: Optional[int] = None, *_: Union[int, str, None,],) -> str:
    """Read string from Native Format."""

    if not lens:
        lens: int = read_lens(file)

    if lens == 0:
        return ""

    return file.read(lens).decode("utf-8")


def write_string(string: str, file: BufferedIOBase, lens: Optional[int] = None, *_: Union[int, str, None,],) -> None:
    """Write string into Native Format."""

    byte_str: bytes = string.encode("utf-8")

    if not lens:
        lens: int = len(byte_str)
        write_lens(lens, file)

    if lens == 0:
        return  # Чтобы не писать в файл пустоту

    file.write(byte_str)
