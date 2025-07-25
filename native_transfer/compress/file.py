from gzip import GzipFile
from io import (
    BufferedIOBase,
    BufferedReader,
    BufferedWriter,
    BytesIO,
)
from logging import Logger
from typing import (
    Optional,
    Union,
)

from .codec import CompressCodec
from .structs import FileBlocks


class NativeCompressFile:
    """Class for unpacking and packing blocks directly."""

    def __init__(
        self,
        file: Union[
            BufferedIOBase,
            BufferedReader,
            BufferedWriter,
            BytesIO,
            GzipFile,
        ],
        codec: CompressCodec,
        logs: Logger,
    ) -> None:
        """Initializing a class."""

        self.file = file
        self.codec = codec
        self.logs = logs
        self.buffer = BytesIO()
        self.file_blocks: FileBlocks = FileBlocks.from_file(file)
        [
            self.buffer.write(
                self.codec.decompress_block(block)
            )
            for block in self.file_blocks.block_list
        ]
        self.buffer.seek(0)
        self.logs.info(self.file_blocks)

    def __enter__(self) -> "NativeCompressFile":
        """Launch the context manager."""

        return self

    def __exit__(
        self,
        *_: object,
    ) -> None:
        """Exit context manager."""

        self.close()

    @property
    def name(self) -> str:
        """Name property func."""

        return self.file.name

    def close(self) -> None:
        """Close func."""

        self.flush()
        self.file.close()
        self.buffer.close()
        del self.file_blocks, self.buffer

    def fileno(self) -> Optional[int]:
        """Fileno func."""

        if not isinstance(self.file, BytesIO):
            return self.file.fileno()

    def flush(self) -> None:
        """Flush func."""

        self.file.flush()

    def readable(self) -> bool:
        """Check readable."""

        return True

    def writable(self) -> bool:
        """Check writable."""

        return True

    def seekable(self) -> bool:
        """Check seekable."""

        return True

    def tell(self) -> int:
        """Tell func."""

        return self.buffer.tell()

    def seek(
        self,
        position: int,
        stop: int = 0,
    ) -> int:
        """Seek func."""

        return self.buffer.seek(
            position,
            stop,
        )

    def read(
        self,
        lenghts: int = -1
    ) -> bytes:
        """Read func."""

        return self.buffer.read(lenghts)

    def write(
        self,
        buffer: Union[bytes, bytearray],
    ) -> int:
        """Write func."""

        self.file.write(
            self.codec.compress_block(
                bytes(buffer)
            ).to_bytes()
        )
        return len(buffer)
