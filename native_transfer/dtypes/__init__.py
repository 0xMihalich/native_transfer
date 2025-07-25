from datetime import (
    date,
    datetime,
)
from ipaddress import (
    IPv4Address,
    IPv6Address,
)
from re import (
    Match,
    search,
)
from typing import (
    Dict,
    Optional,
    Union,
)
from uuid import UUID

from .arrays import Array
from .booleans import (
    read_bool,
    read_nothing,
    read_nullable,
    write_bool,
    write_nothing,
    write_nullable,
)
from .dates import (
    read_date,
    read_date32,
    read_datetime,
    read_datetime64,
    write_date,
    write_date32,
    write_datetime,
    write_datetime64,
)
from .decimals import (
    read_decimal,
    write_decimal,
)
from .struct import DType
from .enums import (
    parse_enum,
    read_enum8,
    read_enum16,
    write_enum8,
    write_enum16,
)
from .floats import (
    read_bfloat16,
    read_float32,
    read_float64,
    write_bfloat16,
    write_float32,
    write_float64,
)
from .integers import (
    read_int,
    read_uint,
    write_int,
    write_uint,
    INTEGER_LENS,
)
from .ipaddrs import (
    read_ipv4,
    read_ipv6,
    write_ipv4,
    write_ipv6,
)
from .lowcardinality import LowCardinality
from .strings import (
    read_string,
    write_string,
)
from .uuids import (
    read_uuid,
    write_uuid,
)

from ..errors import NativeDTypeError


def get_dtype(
    raw_string: str,
    total_rows: Optional[int] = None,
) -> Union[Array, DType, LowCardinality]:
    """Get DType object to work with specified data type.."""

    pattern: str = r"^(\w+)(?:\((.*?)\))?$"
    match: Optional[Match] = search(pattern, raw_string)

    if not match:
        raise NativeDTypeError("Invalid type format.")

    dtype: str = match.group(1)

    # Провести рефактор этого места. Когда-нибудь)

    if dtype == "Array":
        return Array(get_dtype(match.group(2)), total_rows)
    elif dtype == "Bool":
        return DType(dtype, bool, read_bool, write_bool, total_rows, 1)
    elif dtype == "Nullable":
        return DType(
            dtype,
            bool,
            read_nullable,
            write_nullable,
            total_rows,
            1,
            nullables=get_dtype(match.group(2), total_rows),
        )
    elif dtype == "Nothing":
        return DType(
            dtype, type(None), read_nothing, write_nothing, total_rows, 1
        )
    elif dtype == "Date":
        return DType(dtype, date, read_date, write_date, total_rows, 2)
    elif dtype == "Date32":
        return DType(dtype, date, read_date32, write_date32, total_rows, 4)
    elif dtype == "DateTime":
        return DType(
            dtype,
            datetime,
            read_datetime,
            write_datetime,
            total_rows,
            4,
            tzinfo=match.group(2),
        )
    elif dtype == "DateTime64":
        args: str = match.group(2)
        precission: int = int(args[0])
        tzinfo: str = args[4:-1]
        return DType(
            dtype,
            datetime,
            read_datetime64,
            write_datetime64,
            total_rows,
            8,
            tzinfo=tzinfo,
            precission=precission,
        )
    elif dtype == "Decimal":
        decimal_params: str = match.group(2)
        precission, scale = [
            int(param) for param in decimal_params.split(", ")
        ]
        if 1 <= precission <= 9:
            lens = 4
        elif 10 <= precission <= 18:
            lens = 8
        elif 19 <= precission <= 38:
            lens = 16
        elif 39 <= precission <= 76:
            lens = 32
        return DType(
            dtype,
            float,
            read_decimal,
            write_decimal,
            total_rows,
            lens,
            precission=precission,
            scale=scale,
        )
    elif dtype in ("Enum8", "Enum16"):
        enum: Dict[int, str] = parse_enum(raw_string)
        if dtype == "Enum8":
            return DType(dtype, enum, read_enum8, write_enum8, total_rows, 1)
        return DType(dtype, enum, read_enum16, write_enum16, total_rows, 2)
    elif dtype == "BFloat16":
        return DType(
            dtype, float, read_bfloat16, write_bfloat16, total_rows, 2
        )
    elif dtype == "Float32":
        return DType(dtype, float, read_float32, write_float32, total_rows, 4)
    elif dtype == "Float64":
        return DType(dtype, float, read_float64, write_float64, total_rows, 8)
    elif dtype == "IPv4":
        return DType(dtype, IPv4Address, read_ipv4, write_ipv4, total_rows, 4)
    elif dtype == "IPv6":
        return DType(dtype, IPv6Address, read_ipv6, write_ipv6, total_rows, 16)
    elif dtype == "LowCardinality":
        return LowCardinality(match.group(2), total_rows)
    elif dtype == "String":
        return DType(dtype, str, read_string, write_string, total_rows)
    elif dtype == "FixedString":
        lens: int = int(match.group(2))
        return DType(dtype, str, read_string, write_string, total_rows, lens)
    elif dtype == "UUID":
        return DType(dtype, UUID, read_uuid, write_uuid, total_rows, 16)
    elif dtype[:8] == "Interval":
        return DType(dtype, int, read_int, write_int, total_rows, 8)
    elif dtype[:3] == "Int":
        lens: int = INTEGER_LENS[dtype]
        return DType(dtype, int, read_int, write_int, total_rows, lens)
    elif dtype[:4] == "UInt":
        lens: int = INTEGER_LENS[dtype]
        return DType(dtype, int, read_uint, write_uint, total_rows, lens)
