from io import BufferedIOBase
from struct import (
    pack,
    unpack,
)
from typing import Union


def read_bool(
    file: BufferedIOBase,
    *_: Union[
        int,
        str,
        None,
    ],
) -> bool:
    """Read Bool from Native Format."""

    return unpack("<?", file.read(1))[0]


def write_bool(
    boolean: Union[bool, int],
    file: BufferedIOBase,
    *_: Union[
        int,
        str,
        None,
    ],
) -> None:
    """Write Bool into Native Format."""

    file.write(pack("<?", bool(boolean)))


def read_nullable(
    file: BufferedIOBase,
    *_: Union[
        int,
        str,
        None,
    ],
) -> bool:
    """Read Nullable from Native Format."""

    return not read_bool(file)


def write_nullable(
    boolean: Union[bool, int],
    file: BufferedIOBase,
    *_: Union[
        int,
        str,
        None,
    ],
) -> None:
    """Write Nullable into Native Format."""

    write_bool(not boolean, file)


def read_nothing(
    file: BufferedIOBase,
    *_: Union[
        int,
        str,
        None,
    ],
) -> None:
    """Read Nullable(Nothing) from Native Format."""

    # Тут задача не прочитать, а сдвинуть позицию в файле на следующий элемент
    file.read(
        1
    )


def write_nothing(
    *args: Union[int, str, None, BufferedIOBase],
) -> None:
    """Write Nullable(Nothing) into Native Format."""

    file: BufferedIOBase = args[1]
    file.write(b"0")  # Записать значение 0 для Nothing
