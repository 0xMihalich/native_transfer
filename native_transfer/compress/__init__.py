"""Library for working with Clickhouse Native format with compression."""

from .codec import CompressCodec
from .enums import CompressionMethod
from .errors import (
    NativeCompressError,
    NativeCompressMethodNotSupport,
    NativeCompressFileError,
    NativeCompressFormatNotSupport,
    NativeCompressExtractError,
    NativeCompressPackError,
)
from .file import NativeCompressFile
from .structs import (
    BlockStruct,
    FileBlocks,
)


__all__ = (
    "BlockStruct",
    "CompressCodec",
    "CompressionMethod",
    "FileBlocks",
    "NativeCompressError",
    "NativeCompressExtractError",
    "NativeCompressFile",
    "NativeCompressFileError",
    "NativeCompressFormatNotSupport",
    "NativeCompressMethodNotSupport",
    "NativeCompressPackError",
)
