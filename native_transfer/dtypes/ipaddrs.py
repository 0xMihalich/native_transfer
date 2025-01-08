from ipaddress import (
    ip_address,
    IPv4Address,
    IPv6Address,
)
from io import BufferedIOBase
from typing import Union


def read_ipv4(file: BufferedIOBase, *_: Union[int, str, None,],) -> IPv4Address:
    """Прочитать IPv4 из Native Format."""

    return ip_address(file.read(4)[::-1])


def write_ipv4(ipv4: IPv4Address, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Записать IPv4 в Native Format."""

    file.write(ipv4.packed[::-1])


def read_ipv6(file: BufferedIOBase, *_: Union[int, str, None,],) -> IPv6Address:
    """Прочитать IPv6 из Native Format."""

    return ip_address(file.read(16))


def write_ipv6(ipv6: IPv6Address, file: BufferedIOBase, *_: Union[int, str, None,],) -> None:
    """Записать IPv6 в Native Format."""

    file.write(ipv6.packed)
