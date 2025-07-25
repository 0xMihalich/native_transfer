from typing import (
    Any,
    NoReturn,
)


class NativeCompressError(Exception):
    """Basic class error."""


class NativeCompressMethodNotSupport(NativeCompressError):
    """The method is not supported."""


class NativeCompressFileError(TypeError):
    """Unsupported file format."""


class NativeCompressFormatNotSupport(ValueError):
    """Unsupported compression format."""


class NativeCompressExtractError(NativeCompressError):
    """Error retrieving data."""


class NativeCompressPackError(NativeCompressError):
    """Error during compression."""


def unsupported_method(**_: dict[str, Any]) -> NoReturn:
    """Method for returning an error."""

    msg = "This method don't support yet."
    raise NativeCompressMethodNotSupport(msg)
