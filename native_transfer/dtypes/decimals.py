from io import BufferedIOBase
from typing import Any

from .integers import read_int

from ..errors import NativePrecissionError


__doc__ = """
Все Decimals читаются и пишутся как Int8 - Int256.
Независимо от указания альясов в таблицу это пишется как Decimal(P, S).
Для преобразования во Float требуется:
1. Определиться с размером signed integer:
P from [ 1 : 9 ] - Int32
P from [ 10 : 18 ] - Int64
P from [ 19 : 38 ] - Int128
P from [ 39 : 76 ] - Int256
2. получить число из Native как signed integer.
3. число / pow(10, S)
Преобразовывать назад как Decimal не вижу смысла, будем писать в Native как Float32/Float64
"""


def read_decimal(file: BufferedIOBase, *args: Any,) -> float:
    """Прочитать Decimal(P, S) из Native Format."""

    lens: int
    precission: int = args[2]
    scale: int = args[3]

    if 1 <= precission <= 9:
        lens = 4
    elif 10 <= precission <= 18:
        lens = 8
    elif 19 <= precission <= 38:
        lens = 16
    elif 39 <= precission <= 76:
        lens = 32
    else:
        raise NativePrecissionError("precission must be in [1:76] range!")

    decimal: int = read_int(file, lens)

    return decimal / pow(10, scale)
