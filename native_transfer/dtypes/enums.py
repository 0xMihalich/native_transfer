from enum import Enum
from io import BufferedIOBase
from re import (
    findall,
    search,
    Match,
)
from struct import (
    pack,
    unpack,
)
from typing import (
    Dict,
    Optional,
    Union,
)

from ..errors import NativeEnumError


def parse_enum(dtype: str) -> Dict[int, str]:
    """Create Enum8/Enum16 dictionary from string."""

    finder: Optional[Match] = search(r"(Enum8|Enum16)\((.*?)\)", dtype)

    if not finder:
        raise NativeEnumError("Unknown Enum type.")

    return {int(num): strings for strings, num in findall(r"\'(.*?)\' = ([-]*?[0-9]+?),", finder.group(2) + ",")}


def read_enum8(file: BufferedIOBase, *args: Union[int, str, Dict, None,],) -> str:
    """Read Enum8 from Native Format."""

    enum8: Dict[int, str] = args[4]

    return enum8[unpack('<b', file.read(1))[0]]


def write_enum8(enum8: Union[int, Enum], file: BufferedIOBase, *_: Union[int, str, Dict, None,],) -> None:
    """Write Enum8 into Native Format."""

    if isinstance(enum8, Enum):
        enum8: int = enum8.value

    file.write(pack('<b', enum8))


def read_enum16(file: BufferedIOBase, *args: Union[int, str, Dict, None,],) -> str:
    """Read Enum16 from Native Format."""

    enum16: Dict[int, str] = args[4]

    return enum16[unpack('<h', file.read(2))[0]]


def write_enum16(enum16: Union[int, Enum], file: BufferedIOBase, *_: Union[int, str, Enum, None,],) -> None:
    """Write Enum16 into Native Format."""

    if isinstance(enum16, Enum):
        enum16: int = enum16.value

    file.write(pack('<h', enum16))
