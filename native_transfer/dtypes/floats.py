from io import BufferedIOBase
from struct import (
    pack,
    unpack,
)
from typing import Union


def pack_bfloat16(num_float: float) -> bytes:
    """Pack float into BFloat16 value."""

    float32: int = unpack('I', pack('f', num_float))[0]

    return float32.to_bytes(4, "little")[-2:]


def unpack_bfloat16(bfloat16: bytes) -> float:
    """Unpack float from BFloat16 value."""

    bits: str = bin(int.from_bytes(bfloat16, byteorder='little'))[2:].zfill(16)
    sign: int = 1 if bits[0] == "0" else -1
    exponent: int = pow(2, int(bits[1:9], 2) - 127)
    mantissa: int = int(bits[9:], 2)
    mant_mult: int = 1

    for b in range(6, -1, -1):
        if mantissa & pow(2, b):
            mant_mult += 1 / pow(2, 7 - b)

    return sign * exponent * mant_mult


def read_bfloat16(file: BufferedIOBase, *_: Union[int, str, None,],) -> float:
    """Read BFloat16 from Native Format."""

    return unpack_bfloat16(file.read(2))


def write_bfloat16(num_float: float, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Write BFloat16 into Native Format."""

    file.write(pack_bfloat16(num_float))


def read_float32(file: BufferedIOBase, *_: Union[int, str, None,],) -> float:
    """Read Float32 from Native Format."""

    return unpack('<f', file.read(4))[0]


def write_float32(num_float: float, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Write Float32 into Native Format."""

    file.write(pack('<f', num_float))


def read_float64(file: BufferedIOBase, *_: Union[int, str, None,],) -> float:
    """Read Float64 from Native Format."""

    return unpack('<d', file.read(8))[0]


def write_float64(num_float: float, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Write Float64 into Native Format."""

    file.write(pack('<d', num_float))
