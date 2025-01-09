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
from .dtypes import get_dtype
from .dtypes.arrays import Array
from .dtypes.lowcardinality import LowCardinality
from .dtypes.struct import DType
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


__all__ = (
    "DataFormat",
    "DataInfo",
    "FrameType",
    "NativeTransfer",
    "NativeDateError",
    "NativeDateTimeError",
    "NativeDTypeError",
    "NativeError",
    "NativeEnumError",
    "NativePrecissionError",
    "NativeReadError",
    "NativeWriteError",
)
__doc__ = readme
__version__ = "0.0.2"


class NativeTransfer:
    """Класс для работы с Clickhouse Native Format."""

    __version__ = __version__

    def __init__(self: "NativeTransfer",
                 block_rows: int = 65_400,
                 logs: Logger = getLogger(__name__)) -> None:
        """Инициализация класса."""

        if not isinstance(block_rows, int):
            raise NativeError("block_rows must be integer.")
        elif not 0 < block_rows and block_rows <= 1_048_576:
            raise NativeError("block_rows must be in range [1:1048576].")

        self.block_rows: int = block_rows
        self.logs: Logger = logs

        self.logs.info(f"NativeTransfer initialized with {self.block_rows} block rows.")

    def __str__(self: "NativeTransfer") -> str:
        """Строковое представление класса."""

        return f"""┌────────────────────────────────┐
│ NativeTransfer ver {self.__version__:<7}     │
╞════════════════════════════════╡
│ Write Rows Per Block : {self.block_rows:<7} │
└────────────────────────────────┘"""

    def __repr__(self: "NativeTransfer") -> str:
        """Строковое представление класса в интерпретаторе."""

        return self.__str__()

    def extract_block(self: "NativeTransfer",
                      file: Union[BufferedIOBase, GzipFile],
                      frame_type: FrameType = FrameType.Pandas,) -> Union[PdFrame, PlFrame]:
        """Прочитать один блок в polars/pandas DataFrame из Native Format."""

        try:
            num_columns: int = read_lens(file)
            total_rows: int = read_lens(file)
            frames: List[Union[PdSeries, PlFrame]] = []

            for _ in range(num_columns):
                name: str = read_string(file)
                raw_string: str = read_string(file)
                block: Union[Array, DType, LowCardinality] = get_dtype(raw_string, total_rows)

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

    def extract(self: "NativeTransfer",
                file: Union[BufferedIOBase, GzipFile],
                frame_type: FrameType = FrameType.Pandas,) -> Union[PdFrame, PlFrame]:
        """Прочитать polars/pandas DataFrame из Native Format."""

        file.seek(0)
        data_frames: List[Union[PdFrame, PlFrame]] = []
        self.logs.info(f"Read DataFrame from Native File {file.name} operation started.")

        while True:
            try:
                data_frames.append(self.extract_block(file, frame_type))
            except EOF:
                break

        self.logs.info(f"Read DataFrame from Native File {file.name} operation success.")

        if frame_type == FrameType.Pandas:
            return pd_concat(data_frames, ignore_index=True)
        elif frame_type == FrameType.Polars:
            return pl_concat(data_frames, how="vertical")

    def make(self: "NativeTransfer",
             frame: Union[PdFrame, PlFrame],
             file: Union[BufferedIOBase, GzipFile],
             columns: Optional[List[str]] = None,
             dtypes: Optional[List[str]] = None,) -> None:
        """Создать Native Format из DataFrame."""

        try:
            self.logs.info(f"Create native file {file.name} from DataFrame started.")
            if not columns:
                self.logs.warning("No columns found. Get column names from DataFrame operation started.")
                columns: List[str] = list(frame.columns)
                self.logs.warning("Get column names from DataFrame operation success.")
            self.logs.info(f"Columns for write: {columns}")

            if not dtypes:
                self.logs.warning("No data types found. Get data types from DataFrame operation started.")
                dtypes: List[str] = dtype_from_frame(frame)
                self.logs.warning("Get data types from DataFrame operation success.")
            self.logs.info(f"Data Types for write: {dtypes}")

            block_rows: int = self.block_rows

            for df in chunk_frame(frame, block_rows):
                total_rows: int = len(df)
                write_lens(len(columns), file)
                write_lens(total_rows, file)

                for idx, column in enumerate(df.columns):
                    raw_string: str = dtypes[idx]
                    write_string(columns[idx], file)
                    write_string(raw_string, file)
                    block: Union[Array, DType, LowCardinality] = get_dtype(raw_string, total_rows)
                    block.write(df[column].to_list(), file)
                    del block

                del df
            self.logs.info(f"Create native file {file.name} from DataFrame success.")
        except Exception as err:
            self.logs.error(err)
            raise NativeWriteError(err)

    @staticmethod
    def open(file: Union[str, PathLike, bytes, BytesIO, BufferedIOBase, BufferedWriter, GzipFile],
             mode: str = "rb",
             write_compressed: bool = False,) -> Union[BufferedIOBase, GzipFile]:
        """Открыть файл для чтения/записи."""

        if isinstance(file, GzipFile):
            file.seek(0)
            return file
        elif isinstance(file, Union[str, PathLike]):
            file = open(file, mode)
        elif isinstance(file, bytes):
            file = BytesIO(bytes)
        elif isinstance(file, Union[BufferedIOBase, BufferedReader, BufferedWriter]):
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

    @staticmethod
    def info(file: Union[BufferedIOBase, BufferedReader, BufferedWriter, GzipFile, PdFrame, PlFrame]) -> DataInfo:
        """Получить информацию о данных."""

        data_value: Optional[int] = FORMAT_VALUES.get(file.__class__)

        if data_value is None:
            raise NativeError("Unsupported Data Format.")

        if data_value in (2, 3):
            columns: List[str] = list(file.columns)
            dtypes: List[str] = dtype_from_frame(file)
            total_rows: int = len(file)

            return get_info(data_value, columns, dtypes, total_rows)

        columns: List[str] = []
        dtypes: List[str] = []
        total_rows: int = 0

        file.seek(0)

        while True:
            try:
                num_columns: int = read_lens(file)
                _total_rows: int = read_lens(file)

                for _ in range(num_columns):
                    name: str = read_string(file)
                    raw_string: str = read_string(file)

                    if total_rows == 0:
                        columns.append(name)
                        dtypes.append(raw_string)

                    block: Union[Array, DType, LowCardinality] = get_dtype(raw_string, _total_rows)
                    block.skip(file, _total_rows)

                total_rows += _total_rows
            except EOF:
                break

        return get_info(data_value, columns, dtypes, total_rows)
