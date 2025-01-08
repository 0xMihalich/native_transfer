from datetime import (
    date,
    datetime,
    timedelta,
    timezone,
)
from io import BufferedIOBase
from struct import (
    pack,
    unpack,
)
from typing import Union

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo # type: ignore

from ..errors import (
    NativeDateError,
    NativeDateTimeError,
)


DATA: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)


def unpack_date(days: int) -> date:
    """Получить дату."""

    return (DATA + timedelta(days=days)).date()


def pack_date(dateobj: date) -> int:
    """Вернуть количество дней."""

    return (dateobj - DATA.date()).days


def unpack_datetime(seconds: Union[int, float]) -> datetime:
    """Получить timestamp."""

    return DATA + timedelta(seconds=seconds)


def pack_datetime(datetimeobj: datetime) -> Union[int, float]:
    """Вернуть количество секунд/тиков."""

    return (datetimeobj.astimezone(timezone.utc) - DATA).total_seconds()


def read_date(file: BufferedIOBase, *_: Union[int, str, None,],) -> date:
    """Прочитать Date из Native Format."""

    try:
        return unpack_date(unpack('<H', file.read(2))[0])
    except Exception as err:
        raise NativeDateError(err)


def write_date(dateobj: date, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Записать Date в Native Format."""

    days: int = pack_date(dateobj)

    try:
        file.write(pack('<H', days))
    except Exception as err:
        raise NativeDateError(err)


def read_date32(file: BufferedIOBase, *_: Union[int, str, None,],) -> date:
    """Прочитать Date32 из Native Format."""

    try:
        return unpack_date(unpack('<l', file.read(4))[0])
    except Exception as err:
        raise NativeDateError(err)


def write_date32(dateobj: date, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Записать Date32 в Native Format."""

    days: int = pack_date(dateobj)

    try:
        file.write(pack('<l', days))
    except Exception as err:
        raise NativeDateError(err)


def read_datetime(file: BufferedIOBase, *args: Union[int, str, None,],) -> datetime:
    """Прочитать DateTime из Native Format."""

    try:
        datetimeobj: datetime = unpack_datetime(unpack('<l', file.read(4))[0])
        tzinfo: str = args[1]

        if tzinfo:
            return datetimeobj.astimezone(ZoneInfo(tzinfo))

        return datetimeobj
    except Exception as err:
        raise NativeDateTimeError(err)


def write_datetime(datetimeobj: datetime, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Записать DateTime в Native Format."""

    seconds: int = int(pack_datetime(datetimeobj))

    try:
        file.write(pack('<l', seconds))
    except Exception as err:
        raise NativeDateTimeError(err)


def read_datetime64(file: BufferedIOBase, *args: Union[int, str, None,],) -> datetime:
    """Прочитать DateTime64 из Native Format."""

    try:
        precission: int = args[2]

        if not 0 < precission < 9:
            raise NativeDateTimeError("precission must be in [0:9] range!")
        elif not isinstance(precission, int):
            raise NativeDateTimeError("precission must be an integer!")

        tzinfo: str = args[1]
        seconds: int = unpack('<q', file.read(8))[0]
        datetime64: datetime = unpack_datetime(seconds * pow(10, -precission))

        if tzinfo:
            return datetime64.astimezone(ZoneInfo(tzinfo))

        return datetime64
    except Exception as err:
        raise NativeDateTimeError(err)


def write_datetime64(datetimeobj: datetime, file: BufferedIOBase, *args: Union[int, str, None,],) -> None:
    """Записать DateTime64 в Native Format."""

    precission: int = args[2]
    seconds: int = int(pack_datetime(datetimeobj)) // pow(10, -precission)

    try:
        file.write(pack('<q', seconds))
    except Exception as err:
        raise NativeDateTimeError(err)
