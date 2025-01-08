from io import BufferedIOBase
from struct import (
    pack,
    unpack,
)
from typing import Union
from uuid import UUID


def unpack_uuid(buffer_16b: bytes) -> UUID:
    """Распаковать UUID из байт."""
    
    return UUID(bytes=b''.join(unpack('<8s8s', buffer_16b)[::-1])[::-1])


def pack_uuid(uuid: UUID) -> bytes:
    """Упаковать UUID в байты."""
    
    return pack('<8s8s', uuid.bytes[:8][::-1], uuid.bytes[8:][::-1])


def read_uuid(file: BufferedIOBase, *_: Union[int, str, None,],) -> UUID:
    """Прочитать uuid из Native Format."""

    return unpack_uuid(file.read(16))


def write_uuid(uuid: UUID, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Записать uuid в Native Format."""

    file.write(pack_uuid(uuid))
