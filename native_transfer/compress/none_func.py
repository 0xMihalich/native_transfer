from typing import Any

from .calc_hash import block_hash
from .enums import CompressionMethod
from .structs import BlockStruct


def none_compress(
    block_data: bytes,
    **_: dict[str, Any],
) -> BlockStruct:
    """Pack the block without compression."""

    block_size = len(block_data)
    compressed_size = block_size + 9
    compression_codek = CompressionMethod.NONE
    city_hash_128 = block_hash(
        compression_codek,
        compressed_size,
        block_size,
        block_data,
    )

    return BlockStruct(
        city_hash_128,
        compression_codek,
        compressed_size,
        block_size,
        block_data,
        True,
    )


def none_decompress(block: BlockStruct) -> bytes:
    """Extract data from a block without compression."""

    return block.compressed_data
