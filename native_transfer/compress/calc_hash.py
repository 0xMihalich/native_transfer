from struct import pack

from clickhouse_cityhash.cityhash import CityHash128

from .enums import CompressionMethod


def calc_hash(block: str|bytes) -> bytes:
    """Calculate CityHash128 for bytes."""

    city_hash: int = CityHash128(block)
    city_bytes = city_hash.to_bytes(
        length=16,
        byteorder="big",
    )

    return city_bytes[7::-1] + city_bytes[16:7:-1]


def block_hash(
    compression_codek: CompressionMethod,
    compressed_size: int,
    block_size: int,
    block_data: bytes,
) -> bytes:
    """Calculate CityHash128 for a block."""

    return calc_hash(
        pack(
            "<B2L",
            compression_codek.value,
            compressed_size,
            block_size
        ) + block_data
    )
