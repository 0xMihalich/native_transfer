from gzip import GzipFile
from io import (
    BufferedIOBase,
    BufferedReader,
    BufferedWriter,
    BytesIO,
)
from struct import (
    pack,
    unpack,
)
from typing import (
    NamedTuple,
    Union,
)

from .calc_hash import calc_hash
from .enums import CompressionMethod
from .errors import (
    NativeCompressError,
    NativeCompressFileError,
)


class BlockStruct(NamedTuple):
    """Compressed block structure."""

    city_hash_128: bytes
    compression_codek: CompressionMethod
    compressed_size: int
    block_size: int
    compressed_data: bytes
    is_valid: bool

    def __str__(self) -> str:
        """String representation of the block."""

        hash_status = "Valid" if self.is_valid else "Broken"

        return f"""hash: 0x{self.city_hash_128.hex().upper()}
hash status: {hash_status}
codek: {self.compression_codek.name}
compressed block size: {self.compressed_size} bytes
decompressed data size: {self.block_size} bytes"""

    def to_bytes(self) -> bytes:
        """Return block as bytes."""

        return pack(
            f"<16sB2L{len(self.compressed_data)}s",
            self.city_hash_128,
            self.compression_codek.value,
            self.compressed_size,
            self.block_size,
            self.compressed_data,
        )

    @classmethod
    def from_file(
        cls,
        file: Union[BytesIO, BufferedReader, BufferedWriter],
    ) -> "BlockStruct":
        """Extract a block from a file."""

        if not isinstance(
            file,
            Union[BytesIO, BufferedReader, BufferedWriter],
        ):
            msg = f"Unsupported file type {file.__class__}"
            raise NativeCompressFileError(msg)

        city_hash_128 = file.read(16)

        if len(city_hash_128) < 16:
            msg = "EOF"
            raise NativeCompressError(msg)

        compression_codek = CompressionMethod(*unpack("B", file.read(1)))
        compressed_size, *_ = unpack("<L", file.read(4))
        block_size, *_ = unpack("<L", file.read(4))
        compressed_data = file.read(compressed_size - 9)
        file.seek(file.tell() - compressed_size)
        is_valid = calc_hash(file.read(compressed_size)) == city_hash_128

        return cls(
            city_hash_128,
            compression_codek,
            compressed_size,
            block_size,
            compressed_data,
            is_valid,
        )


class FileBlocks(NamedTuple):
    """Blocks of native compressed file."""

    compressed_size: int
    full_size: int
    total_blocks: int
    block_list: list[BlockStruct]

    def __str__(self) -> str:
        """String representation of the file."""

        codecs = {
            blk_lst.compression_codek.name
            for blk_lst in self.block_list
        }
        codec = "" if len(codecs) == 0 else next(
            iter(codecs)
        ) if len(codecs) == 1 else f"Mixed{sorted(codecs)}"
        is_valid = {
            blk_lst.is_valid
            for blk_lst in self.block_list
        }
        hash_status = "Valid" if len(is_valid) == 1 and next(
            iter(is_valid)
        ) else "Broken"

        return f"""compressed size: {self.compressed_size} bytes
decompressed size: {self.full_size} bytes
codek: {codec}
all blocks hash status: {hash_status}
total blocks: {self.total_blocks}"""

    def write_file(
        self,
        file: Union[BytesIO, BufferedReader, BufferedWriter],
    ) -> None:
        """Write a FileBlocks object to a file."""

        if not isinstance(
            file,
            Union[BytesIO, BufferedReader, BufferedWriter],
        ):
            msg = f"Unsupported file type {file.__class__}"
            raise NativeCompressFileError(msg)

        [
            file.write(block.to_bytes())
            for block in self.block_list
        ]

    def to_bytes(self) -> bytes:
        """Return file as bytes."""

        buffer = BytesIO()
        self.write_file(buffer)

        return buffer.getvalue()

    @classmethod
    def from_file(
        cls,
        file: Union[
            BufferedIOBase,
            BufferedReader,
            BufferedWriter,
            BytesIO,
            GzipFile,
        ],
    ) -> "FileBlocks":
        """Get a FileBlocks object from a file."""

        if not isinstance(
            file,
            Union[
                BufferedIOBase,
                BufferedReader,
                BufferedWriter,
                BytesIO,
                GzipFile,
            ],
        ):
            msg = f"Unsupported file type {file.__class__}"
            raise NativeCompressFileError(msg)

        full_size = 0
        total_blocks = 0
        block_list = []

        if isinstance(file, BufferedWriter):
            return cls(
                0,
                full_size,
                total_blocks,
                block_list,
            )

        while True:
            try:
                block_struct = BlockStruct.from_file(file)
                block_list.append(block_struct)
                total_blocks += 1
                full_size += block_struct.block_size
            except NativeCompressError:
                break

        return cls(
            file.tell(),
            full_size,
            total_blocks,
            block_list,
        )
