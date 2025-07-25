from typing import (
    Dict,
    Generator,
    Union,
)

from pandas import DataFrame as PdFrame
from polars import DataFrame as PlFrame

from .errors import dtype_error


def chunk_pandas(
    frame: PdFrame, block_rows: int
) -> Generator[
    PdFrame,
    None,
    None,
]:
    """Split pandas.DataFrame into blocks."""

    for i in range(0, frame.shape[0], block_rows):
        yield frame[i : i + block_rows]


def chunk_polars(
    frame: PlFrame, block_rows: int
) -> Generator[
    PlFrame,
    None,
    None,
]:
    """Split polars.DataFrame into blocks."""

    for df in frame.iter_slices(n_rows=block_rows):
        yield df


FRAME_CHUNKS: Dict[type, object] = {
    PdFrame: chunk_pandas,
    PlFrame: chunk_polars,
}


def chunk_frame(
    frame: Union[PdFrame, PlFrame], block_rows: int
) -> Generator[
    Union[PdFrame, PlFrame],
    None,
    None,
]:
    """Split DataFrame into blocks."""

    return FRAME_CHUNKS.get(frame.__class__, dtype_error)(frame, block_rows)
