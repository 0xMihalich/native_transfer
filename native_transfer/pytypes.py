from enum import Enum
from datetime import (
    date,
    datetime,
    timezone,
)
from ipaddress import (
    IPv4Address,
    IPv6Address,
)
from typing import (
    Any,
    Dict,
    List,
    Tuple,
    Union,
)
from uuid import UUID

from numpy import generic
from pandas import (
    DataFrame as PdFrame,
    Timestamp,
)
from polars import (
    col,
    max,
    min,
    DataFrame as PlFrame,
)
from polars.exceptions import InvalidOperationError

from .errors import dtype_error


PYTYPES: Dict[Tuple[type, int], str] = {
    (str, 0): "String",
    (str, 1): "FixedString",
    (date, 0): "Date",
    (date, 1): "Date32",
    (datetime, 0): "DateTime",
    (datetime, 1): "DateTime64",
    (int, 0): "UInt8",
    (int, 1): "UInt16",
    (int, 2): "UInt32",
    (int, 3): "UInt64",
    (int, 4): "UInt128",
    (int, 5): "UInt256",
    (int, 6): "Int8",
    (int, 7): "Int16",
    (int, 8): "Int32",
    (int, 9): "Int64",
    (int, 10): "Int128",
    (int, 11): "Int256",
    (float, 0): "Float32",
    (float, 1): "Float64",
    (bool, 0): "Bool",
    (UUID, 0): "UUID",
    (IPv4Address, 0): "IPv4",
    (IPv6Address, 0): "IPv6",
}

TZONES: Dict[str, str] = {
    '+0000': 'UTC',
    '+0100': 'Europe/Amsterdam',
    '+0200': 'Europe/Kaliningrad',
    '+0300': 'Europe/Moscow',
    '+0330': 'Asia/Tehran',
    '+0400': 'Europe/Samara',
    '+0430': 'Asia/Kabul',
    '+0500': 'Asia/Yekaterinburg',
    '+0530': 'Asia/Colombo',
    '+0545': 'Asia/Katmandu',
    '+0600': 'Asia/Omsk',
    '+0630': 'Asia/Yangon',
    '+0700': 'Asia/Krasnoyarsk',
    '+0800': 'Asia/Irkutsk',
    '+0845': 'Australia/Eucla',
    '+0900': 'Asia/Yakutsk',
    '+0930': 'Australia/Darwin',
    '+1000': 'Asia/Vladivostok',
    '+1030': 'Australia/Yancowinna',
    '+1100': 'Asia/Magadan',
    '+1200': 'Asia/Kamchatka',
    '+1300': 'Pacific/Enderbury',
    '+1345': 'Pacific/Chatham',
    '+1400': 'Pacific/Kiritimati',
    '-0100': 'Atlantic/Azores',
    '-0200': 'America/Noronha',
    '-0300': 'America/Araguaina',
    '-0400': 'America/Antigua',
    '-0430': 'Canada/Newfoundland',
    '-0500': 'America/Panama',
    '-0600': 'America/Chicago',
    '-0700': 'America/Boise',
    '-0800': 'America/Tijuana',
    '-0900': 'America/Anchorage',
    '-1000': 'America/Adak',
    '-1030': 'Pacific/Marquesas',
    '-1100': 'Pacific/Samoa',
    '-1200': 'Etc/GMT+12',
}


def make_dtype(min_val: Any,
               max_val: Any,
               is_fixed: bool,
               is_nullable: bool,) -> str:
    """Сформировать строку DType."""
    if is_nullable:
        return f"Nullable({make_dtype(min_val, max_val, is_fixed, False)})"
    elif isinstance(max_val, Enum):
        values: str = ", ".join(f"'{i.name}' = {i.value}" for i in max_val.__class__)
        if -128 <= min_val and max_val <= 127:
            return f"Enum8({values})"
        return f"Enum16({values})"
    elif isinstance(max_val, str) and is_fixed:
        return f"{PYTYPES[(str, is_fixed)]}({len(max_val)})"
    elif isinstance(max_val, float):
        if 1.401298464324817e-45 <= min_val and max_val <= 3.4028234663852886e+38:
            return PYTYPES[(float, 0)]
        return PYTYPES[(float, 1)]
    elif isinstance(max_val, datetime):
        if (datetime(1970, 1, 1, tzinfo=timezone.utc) <= min_val.astimezone(timezone.utc)
            and max_val.astimezone(timezone.utc) <= datetime(2106, 2, 7, 6, 28, 15, tzinfo=timezone.utc)):
            return PYTYPES[(datetime, 0)]
        zone: str = TZONES.get(max_val.strftime("%z"), "UTC")
        return f"{PYTYPES[(datetime, 1)]}(3, '{zone}')"
    elif isinstance(max_val, date):
        if date(1970, 1, 1) <= min_val and max_val <= date(2149, 6, 6):
            return PYTYPES[(date, 0)]
        return PYTYPES[(date, 1)]
    elif isinstance(max_val, int):
        if 0 <= min_val:
            if max_val <= 255:
                val = 0
            elif max_val <= 65535:
                val = 1
            elif max_val <= 4294967295:
                val = 2
            elif max_val <= 18446744073709551615:
                val = 3
            elif max_val <= 340282366920938463463374607431768211455:
                val = 4
            else:
                val = 5
        else:
            if -128 <= min_val and 127 <= max_val:
                val = 6
            if -32768 <= min_val and 32767 <= max_val:
                val = 7
            if -2147483648 <= min_val and 2147483647 <= max_val:
                val = 8
            if -9223372036854775808 <= min_val and 9223372036854775807 <= max_val:
                val = 9
            if -170141183460469231731687303715884105728 <= min_val and 170141183460469231731687303715884105727 <= max_val:
                val = 10
            else:
                val = 11
        return PYTYPES[(type(max_val), val)]

    raw_string = PYTYPES.get((type(max_val), is_fixed))

    if not raw_string:
        return "Nothing"

    return raw_string


def dtype_from_polars(frame: PlFrame) -> List[str]:
    """Автоматически определить типы данных Clickhouse для polars.DataFrame."""

    columns: List[str] = frame.columns
    dtypes: List[str] = []

    for column in columns:
        is_nullable: bool = frame.select(col(column).is_null().any()).item()
        is_fixed: bool = False
        min_val: Any
        max_val: Any

        try:
            min_val = frame.select(min(column)).item()
            max_val = frame.select(max(column)).item()
        except InvalidOperationError:
            try:
                min_val, *_, max_val = frame[column].drop_nulls().to_list()
            except ValueError:
                try:
                    min_val = max_val = frame[column].drop_nulls().to_list()[0]
                except ValueError:
                    min_val = max_val = None
                    is_nullable = True

        if isinstance(min_val, str) and len(min_val) == len(max_val):
            is_fixed: bool = frame.filter(col(column).is_not_null()).select(
                (col(column).str.len_chars() == len(max_val)).all()
            ).item()

        if isinstance(min_val, list) or isinstance(max_val, list):
            values: List[Any] = sum(frame[column].to_list(), [])
            is_nullable = None in values
            min_val, *_, max_val = sorted(value for value in values if value is not None)
            del values
            dtypes.append(f"Array({make_dtype(min_val, max_val, is_fixed, is_nullable)})")
            continue

        dtypes.append(make_dtype(min_val, max_val, is_fixed, is_nullable))

    return dtypes


def dtype_from_pandas(frame: PdFrame) -> List[str]:
    """Автоматически определить типы данных Clickhouse для pandas.DataFrame."""

    columns: List[str] = list(frame.columns)
    dtypes: List[str] = []

    for column in columns:
        is_nullable: bool = frame[column].isnull().all()
        is_fixed: bool = False
        min_val: Any
        max_val: Any

        try:
            min_val = frame[column].dropna().min()
            max_val = frame[column].dropna().max()
        except (TypeError, ValueError):
            try:
                min_val, *_, max_val = frame[column].dropna().to_list()
            except ValueError:
                try:
                    min_val = max_val = frame[column].dropna().to_list()[0]
                except ValueError:
                    min_val = max_val = None
                    is_nullable = True

        if isinstance(min_val, str) and len(min_val) == len(max_val):
            is_fixed: bool = (frame[column].str.len() == len(max_val)).all()
        elif isinstance(min_val, Timestamp) or isinstance(max_val, Timestamp):
            min_val = min_val.to_pydatetime()
            max_val = max_val.to_pydatetime()
        elif isinstance(min_val, generic) or isinstance(max_val, generic):
            min_val = min_val.item()
            max_val = max_val.item()
        
        if isinstance(min_val, list) or isinstance(max_val, list):
            values: List[Any] = sum(frame[column].dropna().to_list(), [])
            is_nullable = None in values
            min_val, *_, max_val = sorted(value for value in values if value is not None)
            del values
            dtypes.append(f"Array({make_dtype(min_val, max_val, is_fixed, is_nullable)})")
            continue

        dtypes.append(make_dtype(min_val, max_val, is_fixed, is_nullable))

    return dtypes


SELECT_FRAME: Dict[type, object] = {
    PdFrame: dtype_from_pandas,
    PlFrame: dtype_from_polars,
}

def dtype_from_frame(frame: Union[PdFrame, PlFrame]) -> List[str]:

    return SELECT_FRAME.get(frame.__class__, dtype_error)(frame)
