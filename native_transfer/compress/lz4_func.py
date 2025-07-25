from typing import Any

from lz4.block import (
    compress,
    decompress,
    LZ4BlockError,
)

from .calc_hash import block_hash
from .enums import CompressionMethod
from .structs import BlockStruct


def lz4_compress(
    block_data: bytes,
    **_: dict[str, Any],
) -> BlockStruct:
    """Pack the block into LZ4."""

    block_size = len(block_data)
    compressed_data = compress(block_data)
    compressed_size = len(compressed_data) + 9
    compression_codek = CompressionMethod.LZ4
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


def lz4_decompress(block: BlockStruct) -> bytes:
    """Extract data from LZ4 block."""

    try:
        return decompress(
            block.compressed_data,
            uncompressed_size=block.block_size,
        )
    except LZ4BlockError:
        return decompress(block.compressed_data)
