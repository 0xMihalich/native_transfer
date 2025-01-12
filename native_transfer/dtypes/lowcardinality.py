from datetime import (
    date,
    datetime,
)
from io import BufferedIOBase
from re import (
    Match,
    search,
)
from typing import (
    Any,
    Dict,
    List,
    NoReturn,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from .dates import (
    read_date,
    read_datetime,
)
from .floats import (
    read_bfloat16,
    read_float32,
    read_float64,
)
from .integers import (
    read_int,
    read_uint,
    INTEGER_LENS,
)
from .strings import read_string

from ..errors import NativeDTypeError
from ..lens import read_lens


__doc__ = """
Reading data from LowCardinality block:
0. Supported data types: String, FixedString, Date, DateTime, and numbers excepting Decimal.
1. The number of rows in the header is ignored when working with this format.
2. Skip the 16-byte block; it will not participate in the parser.
3. Read the total number of unique elements in the block as UInt64 (8 bytes).
4. Based on the number obtained in point 3, determine the size of the index:
UInt8   (1 byte)   — [0 : 255]
UInt16  (2 bytes)  — [0 : 65535]
UInt32  (4 bytes)  — [0 : 4294967295]
UInt64  (8 bytes)  — [0 : 18446744073709551615]
UInt128 (16 bytes)  — [0 : 340282366920938463463374607431768211455]
UInt256 (32 bytes) — [0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935]
5. Read all elements as a dictionary: key = index starting from 0, value = element.
The first element always writes the default value for the specified data type.
If Nullable is additionally specified [for example, LowCardinality(Nullable(String))], the first two values will be default,
but the element with index 0 corresponds to None, and the element with index 1 corresponds to the default value for this data type (an empty string).
6. Read the total number of elements in the block as UInt64 (8 bytes). This parameter corresponds to the number of rows in the header.
7. Read the index of each element according to the size obtained in point 4 and relate it to the value in the dictionary.
I see no point in packing all this back, so this class will be intended only for reading from Native Format.
"""

LCType = TypeVar('LCType', str, date, datetime, int, float, None,)


class LowCardinality:
    """Class for unpacking data from the LowCardinality block into a regular Data Type
       (String, FixedString, Date, DateTime, and numbers excepting Decimal)."""
    
    def __init__(self: "LowCardinality", raw_string: str, total_rows: Optional[int],) -> None:
        """Class initialization."""

        pattern: str = r"^(\w+)(?:\((.*?)\))?$"
        match: Optional[Match] = search(pattern, raw_string)

        if not match:
            raise NativeDTypeError("Invalid LowCardinality parameters.")

        if match.group(1) == "Nullable":
            self.nullable: bool = True
            match: Optional[Match] = search(pattern, match.group(2))
        else:
            self.nullable: bool = False

        self.name: str = match.group(1)  # String, FixedString, Date, DateTime, and numbers excepting Decimal
        self.dtype: LCType = None
        self.read_func: Optional[object] = None
        self.lens: Optional[int] = None
        self.tzinfo: Optional[str] = None

        if self.name == "String":
            self.dtype = str
            self.read_func = read_string
        elif self.name == "FixedString":
            self.dtype = str
            self.lens = int(match.group(2))
            self.read_func = read_string
        elif self.name == "Date":
            self.dtype = date
            self.lens = 2
            self.read_func = read_date
        elif self.name == "DateTime":
            self.dtype = datetime
            self.lens = 4
            self.tzinfo = match.group(2)
            self.read_func = read_datetime
        elif self.name[:3] == "Int":
            self.dtype = int
            self.lens: int = INTEGER_LENS[self.name]
            self.read_func = read_int
        elif self.name[:4] == "UInt":
            self.dtype = int
            self.lens: int = INTEGER_LENS[self.name]
            self.read_func = read_uint
        elif self.name == "BFloat16":
            self.dtype = float
            self.lens = 2
            self.read_func = read_bfloat16
        elif self.name == "Float32":
            self.dtype = float
            self.lens = 4
            self.read_func = read_float32
        elif self.name == "Float64":
            self.dtype = float
            self.lens = 8
            self.read_func = read_float64
        else:
            raise NativeDTypeError(f"Invalid LowCardinality type {self.name}.")

        self.index_lens: Optional[int] = None
        self.count_elements: Optional[int] = None
        self.total_rows: Optional[int] = total_rows

    @staticmethod
    def _read_values(file: BufferedIOBase) -> Tuple[int, ...]:
        """Found count_elements and index_lens."""

        file.seek(file.tell() + 16)  # skip header
        count_elements: int = read_uint(file, 8)
        index_lens: int

        # Расчет размера одного индекса

        if count_elements <= 256:
            index_lens = 1
        elif count_elements <= 65536:
            index_lens = 2
        elif count_elements <= 4294967296:
            index_lens = 4
        elif count_elements <= 18446744073709551616:
            index_lens = 8
        elif count_elements <= 340282366920938463463374607431768211456:
            index_lens = 16
        else:
            index_lens = 32

        return count_elements, index_lens

    def read(self: "LowCardinality", file: BufferedIOBase) -> List[LCType]:
        """Read items from LowCardinality block."""

        count_elements, index_lens = self._read_values(file)
        elements: List[LCType] = [self.read_func(file, self.lens, self.tzinfo) for _ in range(count_elements)]

        if self.nullable:
            elements[0] = None  # add null element

        map_elements: Dict[int, LCType] = dict(enumerate(elements))
        del count_elements, elements  # clear memory
        total_count: int = read_uint(file, 8)

        return [map_elements[read_uint(file, index_lens)] for _ in range(total_count)]

    def write(self: "LowCardinality", *_: Union[List[Any], BufferedIOBase]) -> NoReturn:
        """Write functions don't support."""

        raise NativeDTypeError("Write to LowCardinality don't support.")

    def skip(self: "LowCardinality", file: BufferedIOBase, _: Optional[int] = None,) -> None:
        """Skip LowCardinality block."""

        count_elements, index_lens = self._read_values(file)

        if self.lens is None:
            for _ in range(count_elements):
                lens = read_lens(file)
                file.seek(file.tell() + lens)
        else:
            file.seek(file.tell() + (self.lens * count_elements))

        total_count: int = read_uint(file, 8)
        file.seek(file.tell() + (index_lens * total_count))
