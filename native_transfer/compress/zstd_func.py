from zstd import (
    ZSTD_compress,
    ZSTD_uncompress,
)

from .calc_hash import block_hash
from .enums import CompressionMethod
from .structs import BlockStruct


def zstd_compress(
    block_data: bytes,
    compression_level: int,
) -> BlockStruct:
    """Pack the block into ZSTD."""

    block_size = len(block_data)
    compressed_data = ZSTD_compress(
        block_data,
        compression_level,
    )
    compressed_size = len(compressed_data) + 9
    compression_codek = CompressionMethod.ZSTD
    city_hash_128 = block_hash(
        compression_codek,
        compressed_size,
        block_size,
        compressed_data,
    )

    return BlockStruct(
        city_hash_128,
        compression_codek,
        compressed_size,
        block_size,
        compressed_data,
        True,
    )


def zstd_decompress(block: BlockStruct) -> bytes:
    """Extract data from ZSTD block."""

    return ZSTD_uncompress(block.compressed_data)
