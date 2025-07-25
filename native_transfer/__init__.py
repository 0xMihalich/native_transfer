from gzip import GzipFile
from io import (
    BufferedIOBase,
    BufferedReader,
    BufferedWriter,
    BytesIO,
)
from logging import (
    Logger,
    getLogger,
)
from os import PathLike
from struct import error as EOF
from typing import (
    List,
    Optional,
    Union,
    TYPE_CHECKING,
)

from pandas import (
    concat as pd_concat,
    Series as PdSeries,
    DataFrame as PdFrame,
)
from polars import (
    concat as pl_concat,
    DataFrame as PlFrame,
)

from .chunks import chunk_frame
from .compress import (
    BlockStruct,
    CompressCodec,
    CompressionMethod,
    FileBlocks,
    NativeCompressError,
    NativeCompressExtractError,
    NativeCompressFile,
    NativeCompressFileError,
    NativeCompressFormatNotSupport,
    NativeCompressMethodNotSupport,
    NativeCompressPackError,
)
from .dtypes import get_dtype
from .dtypes.strings import (
    read_string,
    write_string,
)
from .errors import (
    NativeDateError,
    NativeDateTimeError,
    NativeDTypeError,
    NativeError,
    NativeEnumError,
    NativePrecissionError,
    NativeReadError,
    NativeWriteError,
)
from .readtypes import FrameType
from .info import (
    DataFormat,
    DataInfo,
    FORMAT_VALUES,
    get_info,
)
from .lens import (
    read_lens,
    write_lens,
)
from .pytypes import dtype_from_frame
from .readme import readme

if TYPE_CHECKING:
    from .dtypes.struct import DType
    from .dtypes.lowcardinality import LowCardinality
    from .dtypes.arrays import Array


__all__ = (
    "BlockStruct",
    "CompressCodec",
    "CompressionMethod",
    "DataFormat",
    "DataInfo",
    "FileBlocks",
    "FrameType",
    "NativeCompressError",
    "NativeCompressExtractError",
    "NativeCompressFile",
    "NativeCompressFileError",
    "NativeCompressFormatNotSupport",
    "NativeCompressMethodNotSupport",
    "NativeCompressPackError",
    "NativeDTypeError",
    "NativeDateError",
    "NativeDateTimeError",
    "NativeEnumError",
    "NativeError",
    "NativePrecissionError",
    "NativeReadError",
    "NativeTransfer",
    "NativeWriteError",
)
__doc__ = readme
__version__ = "0.0.4"


class NativeTransfer:
    """Class for working with Clickhouse Native Format."""

    __version__ = __version__

    def __init__(
        self: "NativeTransfer",
        block_rows: int = 65_400,
        logs: Logger = getLogger(__name__),
        make_compress: bool = False,
        compress_method: CompressionMethod = CompressionMethod.NONE,
        compress_level: int = 0,
    ) -> None:
        """Class initialization."""

        if not isinstance(block_rows, int):
            raise NativeError("block_rows must be integer.")
        if not 0 < block_rows and block_rows <= 1_048_576:
            raise NativeError("block_rows must be in range [1:1048576].")

        self.block_rows = block_rows
        self.make_compress = make_compress
        self.codec = CompressCodec(
            default_method=compress_method,
            default_level=compress_level,
        )
        self.logs = logs

        self.logs.info(
            f"NativeTransfer initialized with {self.block_rows} block rows."
        )

    def __str__(self: "NativeTransfer") -> str:
        """String representation of class."""

        return f"""┌────────────────────────────────┐
│ NativeTransfer ver {self.__version__:<7}     │
╞════════════════════════════════╡
│ Write Rows Per Block : {self.block_rows:<7} │
└────────────────────────────────┘"""

    def __repr__(self: "NativeTransfer") -> str:
        """String representation of class in interpreter."""

        return self.__str__()

    def check_compress(
        self: "NativeTransfer",
        file: Union[BufferedIOBase, GzipFile],
    ) -> Union[BufferedIOBase, GzipFile, NativeCompressFile]:
        """Return NativeCompressFile if compressed."""

        try:
            return NativeCompressFile(
                file=file,
                codec=self.codec,
                logs=self.logs,
            )
        except ValueError:
            """Not a compressed file."""
            return file

    def extract_block(
        self: "NativeTransfer",
        file: Union[BufferedIOBase, GzipFile, NativeCompressFile],
        frame_type: FrameType = FrameType.Pandas,
    ) -> Union[PdFrame, PlFrame]:
        """Read one block from Native Format to polars/pandas DataFrame."""

        try:
            num_columns: int = read_lens(file)
            total_rows: int = read_lens(file)
            frames: List[Union[PdSeries, PlFrame]] = []

            for _ in range(num_columns):
                name: str = read_string(file)
                raw_string: str = read_string(file)
                block: Union[Array, DType, LowCardinality] = get_dtype(
                    raw_string, total_rows
                )

                if frame_type == FrameType.Pandas:
                    frames.append(PdSeries(data=block.read(file), name=name))
                elif frame_type == FrameType.Polars:
                    frames.append(PlFrame({name: block.read(file)}))

            if frame_type == FrameType.Pandas:
                return pd_concat(frames, axis=1)
            elif frame_type == FrameType.Polars:
                return pl_concat(frames, how="horizontal")
        except EOF as err:
            raise err
        except Exception as err:
            self.logs.error(err)
            raise NativeReadError(err)

    def extract(
        self: "NativeTransfer",
        file: Union[BufferedIOBase, GzipFile],
        frame_type: FrameType = FrameType.Pandas,
    ) -> Union[PdFrame, PlFrame]:
        """Read Native Format file to polars/pandas DataFrame."""

        file.seek(0)
        base_file = self.check_compress(file)
        data_frames: List[Union[PdFrame, PlFrame]] = []
        self.logs.info(
            f"Read DataFrame from Native File {file.name} operation started."
        )

        while True:
            try:
                data_frames.append(self.extract_block(base_file, frame_type))
            except EOF:
                break

        self.logs.info(
            f"Read DataFrame from Native File {file.name} operation success."
        )

        if frame_type == FrameType.Pandas:
            return pd_concat(data_frames, ignore_index=True)
        elif frame_type == FrameType.Polars:
            return pl_concat(data_frames, how="vertical")

    def make(
        self: "NativeTransfer",
        frame: Union[PdFrame, PlFrame],
        file: Union[BufferedIOBase, GzipFile],
        columns: Optional[List[str]] = None,
        dtypes: Optional[List[str]] = None,
    ) -> None:
        """Make Native Format from polars/pandas DataFrame."""

        if self.make_compress:
            file = NativeCompressFile(
                file=file,
                codec=self.codec,
                logs=self.logs,
            )

        buffer = BytesIO()

        try:
            self.logs.info(
                f"Create native file {file.name} from DataFrame started."
            )
            if not columns:
                self.logs.warning(
                    "No columns found. Get column names "
                    "from DataFrame operation started."
                )
                columns: List[str] = list(frame.columns)
                self.logs.warning(
                    "Get column names from DataFrame operation success."
                )
            self.logs.info(f"Columns for write: {columns}")

            if not dtypes:
                self.logs.warning(
                    "No data types found. Get data types "
                    "from DataFrame operation started."
                )
                dtypes: List[str] = dtype_from_frame(frame)
                self.logs.warning(
                    "Get data types from DataFrame operation success."
                )
            self.logs.info(f"Data Types for write: {dtypes}")

            block_rows: int = self.block_rows

            for df in chunk_frame(frame, block_rows):
                total_rows: int = len(df)
                write_lens(len(columns), buffer)
                write_lens(total_rows, buffer)

                for idx, column in enumerate(df.columns):
                    raw_string: str = dtypes[idx]
                    write_string(columns[idx], buffer)
                    write_string(raw_string, buffer)
                    block: Union[Array, DType, LowCardinality] = get_dtype(
                        raw_string, total_rows
                    )
                    block.write(df[column].to_list(), buffer)
                    del block

                file.write(buffer.getvalue())
                buffer = BytesIO()
                del df
            self.logs.info(
                f"Create native file {file.name} from DataFrame success."
            )
        except Exception as err:
            self.logs.error(err)
            raise NativeWriteError(err)

    @staticmethod
    def open(
        file: Union[
            str,
            PathLike,
            bytes,
            BytesIO,
            BufferedIOBase,
            BufferedWriter,
            GzipFile,
        ],
        mode: str = "rb",
        write_compressed: bool = False,
    ) -> Union[BufferedIOBase, GzipFile]:
        """Open file for read/write."""

        if isinstance(file, GzipFile):
            file.seek(0)
            return file
        elif isinstance(file, Union[str, PathLike]):
            file = open(file, mode)
        elif isinstance(file, bytes):
            file = BytesIO(bytes)
        elif isinstance(
            file, Union[BufferedIOBase, BufferedReader, BufferedWriter]
        ):
            file.seek(0)
        else:
            raise NativeError("Unsupported file type.")

        if mode == "rb":
            magic: bytes = file.read(2)
            file.seek(0)

            if magic == b"\x1f\x8b":
                return GzipFile(mode=mode, fileobj=file)
        elif mode == "wb":
            if write_compressed:
                return GzipFile(mode=mode, fileobj=file)

        return file

    def info(
        self,
        file: Union[
            BufferedIOBase,
            BufferedReader,
            BufferedWriter,
            GzipFile,
            PdFrame,
            PlFrame,
        ],
    ) -> DataInfo:
        """Get info for input data."""

        base_file = self.check_compress(file)
        data_value: Optional[int] = FORMAT_VALUES.get(base_file.__class__)

        if data_value is None:
            raise NativeError("Unsupported Data Format.")

        if data_value in (2, 3):
            columns: List[str] = list(base_file.columns)
            dtypes: List[str] = dtype_from_frame(base_file)
            total_rows: int = len(base_file)

            return get_info(data_value, columns, dtypes, total_rows)

        columns: List[str] = []
        dtypes: List[str] = []
        total_rows: int = 0

        base_file.seek(0)

        while True:
            try:
                num_columns: int = read_lens(base_file)
                _total_rows: int = read_lens(base_file)

                for _ in range(num_columns):
                    name: str = read_string(base_file)
                    raw_string: str = read_string(base_file)

                    if total_rows == 0:
                        columns.append(name)
                        dtypes.append(raw_string)

                    block: Union[Array, DType, LowCardinality] = get_dtype(
                        raw_string, _total_rows
                    )
                    block.skip(base_file, _total_rows)

                total_rows += _total_rows
            except EOF:
                break

        if isinstance(base_file, NativeCompressFile):
            return f"""{get_info(data_value, columns, dtypes, total_rows)}

Compressed info:
────────────────
{base_file.file_blocks}
"""

        return get_info(data_value, columns, dtypes, total_rows)
