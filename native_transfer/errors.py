from typing import (
    Any,
    NoReturn,
    Union,
)


class NativeError(Exception):
    """Базовая ошибка."""


class NativeReadError(NativeError):
    """Ошибка чтения."""


class NativeWriteError(NativeError):
    """Ошибка записи."""


class NativeEnumError(ValueError):
    """Неверный тип Enum."""


class NativePrecissionError(ValueError):
    """Неверный precission."""


class NativeDTypeError(TypeError):
    """Неверный Data Type."""


class NativeDateError(ValueError):
    """Ошибка при получении Date/Date32."""


class NativeDateTimeError(ValueError):
    """Ошибка при получении DateTime/DateTime64."""


def any_error(any: Any, *_: Union[int, str, None,],) -> NoReturn:
    """Функция вызова любой ошибки."""

    raise NativeError(f"Unknown error {any}.")


def dtype_error(dtype: Any, *_: Union[int, str, None,],) -> NoReturn:
    """Функция вызова ошибки Data Type."""

    raise NativeDTypeError(f"Unsupported operation for Data Type {type(dtype)}.")
