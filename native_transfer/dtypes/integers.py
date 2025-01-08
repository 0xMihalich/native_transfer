from io import BufferedIOBase
from typing import (
    Dict,
    Union,
)


INTEGER_LENS: Dict[str, int] = {
    "UInt8": 1,
    "UInt16": 2,
    "UInt32": 4,
    "UInt64": 8,
    "UInt128": 16,
    "UInt256": 32,
    "Int8": 1,
    "Int16": 2,
    "Int32": 4,
    "Int64": 8,
    "Int128": 16,
    "Int256": 32,
}


def read_int(file: BufferedIOBase, lens: int, *_: Union[int, str, None,],) -> int:
    """Прочитать signed integer из Native Format."""

    return int.from_bytes(file.read(lens), "little", signed=True)


def write_int(num: int, file: BufferedIOBase, lens: int, *_: Union[int, str, None,],) -> None:
    """Записать signed integer в Native Format."""

    file.write(num.to_bytes(lens, "little", signed=True))


def read_uint(file: BufferedIOBase, lens: int, *_: Union[int, str, None,],) -> int:
    """Прочитать unsigned integer из Native Format."""

    return int.from_bytes(file.read(lens), "little", signed=False)


def write_uint(num: int, file: BufferedIOBase, lens: int, *_: Union[int, str, None,],) -> None:
    """Записать unsigned integer в Native Format."""

    file.write(num.to_bytes(lens, "little", signed=False))
