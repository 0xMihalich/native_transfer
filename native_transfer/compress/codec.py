from typing import Optional

from .calc_hash import calc_hash
from .enums import CompressionMethod
from .errors import unsupported_method
from .lz4_func import (
    lz4_compress,
    lz4_decompress,
)
from .none_func import (
    none_compress,
    none_decompress,
)
from .structs import (
    BlockStruct,
    FileBlocks,
)
from .zstd_func import (
    zstd_compress,
    zstd_decompress,
)


# Just so you don't forget about the default
# block value in the original utility
BLOCK_SIZE = 1048576


class CompressCodec:
    """Class for packing and unpacking Native files."""

    def __init__(
        self,
        default_method: CompressionMethod = CompressionMethod.LZ4,
        default_level: int = 0,
    ) -> None:
        """Initialization of the class."""

        self.default_method = default_method
        self.default_level = default_level
        self.compress_selector = {
            CompressionMethod.NONE: none_compress,
            CompressionMethod.LZ4: lz4_compress,
            CompressionMethod.ZSTD: zstd_compress,
        }
        self.decompress_selector = {
            CompressionMethod.NONE: none_decompress,
            CompressionMethod.LZ4: lz4_decompress,
            CompressionMethod.ZSTD: zstd_decompress,
        }

    def compress_block(
        self,
        block_data: bytes,
        method: Optional[CompressionMethod] = None,
        level: Optional[int] = None,
    ) -> BlockStruct:
        """Pack the block."""

        compression_method = method or self.default_method
        compression_level = level or self.default_level
        return self.compress_selector.get(
            compression_method,
            unsupported_method,
        )(
            block_data=block_data,
            compression_level=compression_level,
        )

    def decompress_block(
        self,
        block: BlockStruct,
    ) -> bytes:
        """Unpack the block."""

        return self.decompress_selector.get(
            block.compression_codek,
            unsupported_method,
        )(
            block=block,
        )

    def change_compress_type(
        self,
        block: BlockStruct,
        method: Optional[CompressionMethod] = None,
        level: Optional[int] = None,
    ) -> BlockStruct:
        """Change the compression method to NONE, LZ4, LZ4HC or ZSTD."""

        return self.compress_block(
            block_data=self.decompress_block(block),
            method=method,
            level=level,
        )

    def block_hash_repair(
        self,
        block: BlockStruct,
    ) -> BlockStruct:
        """Recalculate the block checksum."""

        if block.is_valid:
            return block

        return BlockStruct(
            calc_hash(block.to_bytes()[16:]),
            *block[1:-1],
            True,
        )

    def file_hash_repair(
        self,
        file: FileBlocks,
    ) -> FileBlocks:
        """Recalculate the checksum of all blocks."""

        return FileBlocks(
            *file[:-1],
            [
                self.block_hash_repair(block)
                for block in file.block_list
            ],
        )
