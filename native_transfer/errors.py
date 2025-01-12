from typing import (
    Any,
    NoReturn,
    Union,
)


class NativeError(Exception):
    """Base error."""


class NativeReadError(NativeError):
    """Read error."""


class NativeWriteError(NativeError):
    """Write error."""


class NativeEnumError(ValueError):
    """Enum error."""


class NativePrecissionError(ValueError):
    """Value precission error."""


class NativeDTypeError(TypeError):
    """Data Type error."""


class NativeDateError(ValueError):
    """Date/Date32 error."""


class NativeDateTimeError(ValueError):
    """DateTime/DateTime64 error."""


def any_error(any: Any, *_: Union[int, str, None,],) -> NoReturn:
    """Any error raise error function."""

    raise NativeError(f"Unknown error {any}.")


def dtype_error(dtype: Any, *_: Union[int, str, None,],) -> NoReturn:
    """Data Type raise error function."""

    raise NativeDTypeError(f"Unsupported operation for Data Type {type(dtype)}.")
