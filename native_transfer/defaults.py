from datetime import (
    date,
    datetime,
    timezone,
)
from enum import Enum
from ipaddress import IPv4Address, IPv6Address
from typing import (
    Any,
    Dict,
)
from uuid import UUID


NILL_VALUES: Dict[type, Any] = {
    int: 0,
    float: 0,
    str: "",
    date: date(1970, 1, 1),
    datetime: datetime(1970, 1, 1, tzinfo=timezone.utc),
    Enum: 1,
    bool: False,
    UUID: UUID("00000000-0000-0000-0000-000000000000"),
    IPv4Address: IPv4Address("0.0.0.0"),  # noqa: S104
    IPv6Address: IPv6Address("::"),
}


def null_correction(value: Any, dtype: type) -> Any:
    """Replacing None values with default values for current data type."""

    if value is None:
        return NILL_VALUES.get(dtype)

    return value
