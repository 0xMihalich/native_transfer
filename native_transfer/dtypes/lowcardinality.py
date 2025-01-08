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
Чтение данных из блока LowCardinality:
0. Поддерживаемые типы данных: String, FixedString, Date, DateTime и числа за исключением типа Decimal.
1. Количество строк в header при работе с данным форматом игнорируется.
2. Пропускаем блок в 16 байт, он не будет учавствовать в парсере.
3. Читаем общее количество уникальных элементов в блоке как UInt64 (8 байт).
4. На основании числа, полученного в 3 пункте определяем размер индекса:
UInt8   (1 байт)   — [0 : 255]
UInt16  (2 байта)  — [0 : 65535]
UInt32  (4 байта)  — [0 : 4294967295]
UInt64  (8 байт)   — [0 : 18446744073709551615]
UInt128 (16 байт)  — [0 : 340282366920938463463374607431768211455]
UInt256 (32 байта) — [0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935]
5. Читаем все элементы как словарь: ключ = индекс с 0, значение = элемент.
Первым элементом всегда пишется default значение для указанного типа данных.
В случае, если дополнительно указано Nullable [пример LowCardinality(Nullable(String))] первые два значения будут default,
но элемент с индексом 0 соответствует None, элемент с индексом 1 соответствует default значению для данного типа данных (пустая строка).
6. Читаем общее количество элементов в блоке как UInt64 (8 байт). Данный параметр соответствует количеству строк в header
7. Читаем индекс каждого элемента согласно размеру, полученному в пункте 2 и соотносим со значением словаря
Упаковыварь все это назад не вижу смысла, поэтому данный класс будет предназначен только для чтения из Native Format.
"""

LCType = TypeVar('LCType', str, date, datetime, int, float, None,)


class LowCardinality:
    """Класс для распаковки данных блока LowCardinality в обычный Data Type
       (String, FixedString, Date, DateTime и числа за исключением типа Decimal)."""
    
    def __init__(self: "LowCardinality", raw_string: str, total_rows: Optional[int],) -> None:
        """Инициализация класса."""

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
        """Определить count_elements и index_lens."""

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
        """Прочитать элементы из блока LowCardinality."""

        count_elements, index_lens = self._read_values(file)
        elements: List[LCType] = [self.read_func(file, self.lens, self.tzinfo) for _ in range(count_elements)]

        if self.nullable:
            elements[0] = None  # add null element

        map_elements: Dict[int, LCType] = dict(enumerate(elements))
        del count_elements, elements  # clear memory
        total_count: int = read_uint(file, 8)

        return [map_elements[read_uint(file, index_lens)] for _ in range(total_count)]

    def write(self: "LowCardinality", *_: Union[List[Any], BufferedIOBase]) -> NoReturn:
        """Запись не поддерживается."""

        raise NativeDTypeError("Write to LowCardinality don't support.")

    def skip(self: "LowCardinality", file: BufferedIOBase, _: Optional[int] = None,) -> None:
        """Пропустить блок LowCardinality."""

        count_elements, index_lens = self._read_values(file)

        if self.lens is None:
            for _ in range(count_elements):
                lens = read_lens(file)
                file.seek(file.tell() + lens)
        else:
            file.seek(file.tell() + (self.lens * count_elements))

        total_count: int = read_uint(file, 8)
        file.seek(file.tell() + (index_lens * total_count))
