from io import BufferedIOBase
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

from .struct import DType
from .integers import (
    read_uint,
    write_uint,
)


class Array:
    """Class for read and write array items."""

    def __init__(self: "Array", item: DType, total_rows: int) -> None:
        """Class initialization."""

        self.item: DType = item
        self.total_rows: int = total_rows
        self.row_elements: List[Tuple[int, int]] = []
        self.total_items: int = 0

    def read(self: "Array", file: BufferedIOBase) -> List[List[Any]]:
        """Read Arrays."""

        for _ in range(self.total_rows):
            row: int = read_uint(file, 8)
            self.row_elements.append(
                (
                    self.total_items,
                    row,
                )
            )
            self.total_items = row

        item_params: Dict[str, Any] = self.item._asdict()
        item_params["total_rows"] = self.total_items
        self.item: DType = DType(**item_params)
        items: List[Any] = self.item.read(file)

        return [items[start:stop] for start, stop in self.row_elements]

    def write(
        self: "Array", values: List[List[Any]], file: BufferedIOBase
    ) -> None:
        """Write Arrays."""

        self.total_rows: int = len(values)
        num: int = 0

        for value in values:
            num += len(value)
            write_uint(num, file, 8)

        values: List[Any] = sum(values, [])
        self.total_items: int = len(values)
        item_params: Dict[str, Any] = self.item._asdict()
        item_params["total_rows"] = self.total_items
        self.item: DType = DType(**item_params)

        self.item.write(values, file)

    def skip(
        self: "Array",
        file: BufferedIOBase,
        total_count: Optional[int] = None,
    ) -> None:
        """Skip Arrays block."""

        total_rows: int = self.total_rows or total_count
        file.seek(file.tell() + (8 * (total_rows - 1)))
        _total_count: int = read_uint(file, 8)
        self.item.skip(file, _total_count)
