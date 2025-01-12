from io import BufferedIOBase
from typing import Any

from .integers import (
    read_int,
    write_int,
)

from ..errors import NativePrecissionError


__doc__ = """
All Decimals are read and written as Int8 - Int256.
Regardless of the specified aliases in the table, this is written as Decimal(P, S).
To convert to Float, the following is required:
1. Determine the size of the signed integer:
P from [1: 9] - Int32
P from [10: 18] - Int64
P from [19: 38] - Int128
P from [39: 76] - Int256
2. Get the number from Native as a signed integer.
3. Number / pow(10, S)
I see no point in converting back to Decimal; we will write in Native as Float32/Float64.
"""

def calc_lens(precission: int) -> int:
    """Calculate lens."""

    if 1 <= precission <= 9:
        return 4
    elif 10 <= precission <= 18:
        return 8
    elif 19 <= precission <= 38:
        return 16
    elif 39 <= precission <= 76:
        return 32

    raise NativePrecissionError("precission must be in [1:76] range!")


def read_decimal(file: BufferedIOBase, *args: Any,) -> float:
    """Read Decimal(P, S) from Native Format."""

    precission: int = args[2]
    scale: int = args[3]
    lens: int = calc_lens(precission)
    decimal: int = read_int(file, lens)

    return decimal / pow(10, scale)


def write_decimal(decimal: float, file: BufferedIOBase, *args: Any,) -> None:
    """Write Decimal(P, S) into Native Format."""

    precission: int = args[2]
    scale: int = args[3]
    lens: int = calc_lens(precission)

    write_int(int(decimal * pow(10, scale)), file, lens)
