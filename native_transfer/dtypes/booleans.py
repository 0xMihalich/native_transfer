from io import BufferedIOBase
from struct import (
    pack,
    unpack,
)
from typing import Union


def read_bool(file: BufferedIOBase, *_: Union[int, str, None,],) -> bool:
    """Прочитать Bool из Native Format."""

    return unpack('<?', file.read(1))[0]


def write_bool(boolean: Union[bool, int], file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Записать Bool в Native Format."""

    file.write(pack('<?', bool(boolean)))


def read_nullable(file: BufferedIOBase, *_: Union[int, str, None,],) -> bool:
    """Прочитать Nullable из Native Format."""

    return not read_bool(file)


def write_nullable(boolean: Union[bool, int], file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Записать Nullable в Native Format."""

    write_bool(not boolean, file)


def read_nothing(file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Прочитать Nullable(Nothing) из Native Format."""

    file.read(1)  # Тут задача не прочитать, а сдвинуть позицию в файле на следующий элемент


def write_nothing(*args: Union[int, str, None, BufferedIOBase],) -> None:
    """Записать  Nullable(Nothing) в Native Format."""

    file: BufferedIOBase = args[1]
    file.write(b'0')  # Записать значение 0 для Nothing
