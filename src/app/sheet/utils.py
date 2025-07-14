from gspread.utils import a1_range_to_grid_range
from pydantic import BaseModel


class GridRange(BaseModel):
    startRowIndex: int = 0
    endRowIndex: int = 0
    startColumnIndex: int = 0
    endColumnIndex: int = 0


def fri_a1_range_to_grid_range(name: str) -> GridRange:
    return GridRange.model_validate(a1_range_to_grid_range(name))
