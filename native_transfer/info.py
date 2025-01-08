from enum import Enum
from io import (
    BufferedIOBase,
    BufferedReader,
    BufferedWriter,
)
from typing import (
    Dict,
    List,
    NamedTuple,
)
from gzip import GzipFile

from pandas import DataFrame as PdFrame
from polars import DataFrame as PlFrame


FORMAT_VALUES: Dict[type, int] = {
    BufferedIOBase: 0,
    BufferedReader: 0,
    BufferedWriter: 0,
    GzipFile: 1,
    PdFrame: 2,
    PlFrame: 3,
}

class DataFormat(Enum):
    """Формат обрабатываемых данных."""

    Native = 0
    GzipNative = 1
    Pandas = 2
    Polars = 3


class DataInfo(NamedTuple):
    """Информация о формате данных."""

    data_format: DataFormat
    columns: List[str]
    dtypes: List[str]
    total_rows: int

    def __str__(self: "DataInfo") -> str:
        """Строковое представление класса."""

        columns = "\n".join(
            f"{(num + 1):>3}. {col} [ {dtype} ]"
            for num, (col, dtype,)
            in enumerate(zip(self.columns, self.dtypes))
        )

        return f"""Data info:
──────────
Format: {self.data_format.name}
Total columns: {len(self.columns)}
Total rows: {self.total_rows}

Columns description:
────────────────────
{columns}"""

    def __repr__(self: "DataInfo") -> str:
        """Строковое представление класса в интерпретаторе."""

        return self.__str__()


def get_info(data_value: int,
             columns: List[str],
             dtypes: List[str],
             total_rows: int,) -> DataInfo:
    """Сформировать DataInfo."""

    return DataInfo(
        DataFormat(data_value),
        columns,
        dtypes,
        total_rows,
    )
