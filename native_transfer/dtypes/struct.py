from io import BufferedIOBase
from typing import (
    Any,
    List,
    NamedTuple,
    Optional,
)

from ..defaults import null_correction
from ..errors import NativeDTypeError
from ..lens import read_lens


class DType(NamedTuple):
    """Base Data Type struct."""

    name: str
    dtype: type
    read_func: object
    write_func: object
    total_rows: Optional[int] = None
    lens: Optional[int] = None
    tzinfo: Optional[str] = None
    precission: Optional[int] = None
    scale: Optional[int] = None
    nullables: Optional["DType"] = None

    def _read(self: "DType", file: BufferedIOBase) -> Any:
        """Read data from Native Format."""

        return self.read_func(
            file,
            self.lens,
            self.tzinfo,
            self.precission,
            self.scale,
            self.dtype,
        )

    def _write(self: "DType", value: Any, file: BufferedIOBase) -> None:
        """Write data into Native Format."""

        if not isinstance(value, self.dtype):
            if value is not None:
                raise NativeDTypeError(
                    f"DType {type(value)} not match with {self.dtype}."
                )

        self.write_func(
            null_correction(value, self.dtype),
            file,
            self.lens,
            self.tzinfo,
            self.precission,
            self.scale,
            self.dtype,
        )

    def read(self: "DType", file: BufferedIOBase) -> List[Any]:
        """Read block items."""

        if not self.total_rows:
            return []

        if self.nullables:
            not_empty: List[bool] = [
                self._read(file) for _ in range(self.total_rows)
            ]

            def read_nullable(num) -> Any | None:
                value: Any = self.nullables._read(file)

                if not_empty[num]:
                    return value

            return [read_nullable(num) for num in range(self.total_rows)]

        return [self._read(file) for _ in range(self.total_rows)]

    def write(self: "DType", values: List[Any], file: BufferedIOBase) -> None:
        """Write block items."""

        if not self.total_rows:
            return

        if self.nullables:

            def write_nullable(value) -> bool:
                if value is None:
                    return False
                return True

            [self._write(write_nullable(value), file) for value in values]
            [self.nullables._write(value, file) for value in values]
        else:
            [self._write(value, file) for value in values]

    def skip(
        self: "DType",
        file: BufferedIOBase,
        total_count: Optional[int] = None,
    ) -> None:
        """Skip block."""

        total_rows: int = self.total_rows or total_count

        if self.lens is None:
            for _ in range(total_rows):
                lens = read_lens(file)
                file.seek(file.tell() + lens)
        else:
            file.seek(file.tell() + (self.lens * total_rows))

        if self.nullables:
            self.nullables.skip(file, total_rows)
