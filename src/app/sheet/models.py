from datetime import datetime

from typing import Annotated, Final, Self, TypeVar, Generic, Any

from gspread.worksheet import Worksheet
from gspread.utils import ValueInputOption
from pydantic import BaseModel, ConfigDict, ValidationError


from ..shared.decorators import retry_on_fail
from .enums import CheckType
from .g_sheet import gsheet_client
from .exceptions import SheetError
from ..shared.utils import formated_datetime

T = TypeVar("T")

COL_META: Final[str] = "col_name_xxx"
IS_UPDATE_META: Final[str] = "is_update_xxx"
IS_NOTE_META: Final[str] = "is_note_xxx"


class NoteMessageUpdatePayload(BaseModel):
    index: int
    message: str


class BatchCellUpdatePayload(BaseModel, Generic[T]):
    cell: str
    value: T


class ColSheetModel(BaseModel):
    # Model config
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sheet_id: str
    sheet_name: str
    index: int

    @classmethod
    def get_worksheet(
        cls,
        sheet_id: str,
        sheet_name: str,
    ) -> Worksheet:
        """
        Retrieve a worksheet object from Google Sheets using the provided sheet ID and sheet name.
        Args:
            sheet_id (str): The ID of the Google Sheet.
            sheet_name (str): The name of the worksheet within the sheet.
        Returns:
            Worksheet: The worksheet object.
        """
        spreadsheet = gsheet_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)

        return worksheet

    @classmethod
    def mapping_fields(cls) -> dict:
        """
        Get a mapping of model field names to their corresponding column names in the sheet.
        Returns:
            dict: Mapping of field names to column names.
        """
        mapping_fields = {}
        for field_name, field_info in cls.model_fields.items():
            if hasattr(field_info, "metadata"):
                for metadata in field_info.metadata:
                    if COL_META in metadata:
                        mapping_fields[field_name] = metadata[COL_META]
                        break

        return mapping_fields

    @classmethod
    def updated_mapping_fields(cls) -> dict:
        """
        Get a mapping of model field names to column names for fields that are marked as updatable.
        Returns:
            dict: Mapping of updatable field names to column names.
        """
        mapping_fields = {}
        for field_name, field_info in cls.model_fields.items():
            if hasattr(field_info, "metadata"):
                for metadata in field_info.metadata:
                    if (
                        COL_META in metadata
                        and IS_UPDATE_META in metadata
                        and metadata[IS_UPDATE_META]
                    ):
                        mapping_fields[field_name] = metadata[COL_META]
                        break

        return mapping_fields

    @classmethod
    def get(
        cls,
        sheet_id: str,
        sheet_name: str,
        index: int,
    ) -> Self:
        """
        Retrieve a single row from the sheet and return it as a model instance.
        Args:
            sheet_id (str): The ID of the Google Sheet.
            sheet_name (str): The name of the worksheet.
            index (int): The row index to retrieve.
        Returns:
            Self: An instance of the model populated with the row data.
        """
        mapping_dict = cls.mapping_fields()

        query_value = []

        for _, v in mapping_dict.items():
            query_value.append(f"{v}{index}")

        worksheet = cls.get_worksheet(sheet_id=sheet_id, sheet_name=sheet_name)

        model_dict = {
            "index": index,
            "sheet_id": sheet_id,
            "sheet_name": sheet_name,
        }

        query_results = worksheet.batch_get(query_value)
        count = 0
        for k, _ in mapping_dict.items():
            model_dict[k] = query_results[count].first()
            if isinstance(model_dict[k], str):
                model_dict[k] = model_dict[k].strip()
            count += 1
        return cls.model_validate(model_dict)

    @classmethod
    def batch_get(
        cls,
        sheet_id: str,
        sheet_name: str,
        indexes: list[int],
    ) -> list[Self]:
        """
        Retrieve multiple rows from the sheet and return them as a list of model instances.
        Args:
            sheet_id (str): The ID of the Google Sheet.
            sheet_name (str): The name of the worksheet.
            indexes (list[int]): List of row indexes to retrieve.
        Returns:
            list[Self]: List of model instances for each row.
        """
        worksheet = cls.get_worksheet(
            sheet_id=sheet_id,
            sheet_name=sheet_name,
        )
        mapping_dict = cls.mapping_fields()

        result_list: list[Self] = []
        error_list: list[NoteMessageUpdatePayload] = []

        query_value = []
        for index in indexes:
            for _, v in mapping_dict.items():
                query_value.append(f"{v}{index}")

        query_results = worksheet.batch_get(query_value)

        count = 0

        for index in indexes:
            model_dict = {
                "index": index,
                "sheet_id": sheet_id,
                "sheet_name": sheet_name,
            }

            for k, _ in mapping_dict.items():
                model_dict[k] = query_results[count].first()
                if isinstance(model_dict[k], str):
                    model_dict[k] = model_dict[k].strip()
                count += 1
            try:
                result_list.append(cls.model_validate(model_dict))
            except ValidationError as e:
                error_list.append(
                    NoteMessageUpdatePayload(
                        index=index,
                        message=f"{formated_datetime(datetime.now())} Validation Error at row {index}: {e.errors(include_url=False)}",
                    )
                )

        cls.batch_update_note_message(
            sheet_id=sheet_id, sheet_name=sheet_name, update_payloads=error_list
        )

        return result_list

    @classmethod
    @retry_on_fail(max_retries=3, sleep_interval=30)
    def batch_update(
        cls,
        sheet_id: str,
        sheet_name: str,
        list_object: list[Self],
    ) -> None:
        """
        Batch update multiple rows in the sheet with the provided list of model instances.
        Args:
            sheet_id (str): The ID of the Google Sheet.
            sheet_name (str): The name of the worksheet.
            list_object (list[Self]): List of model instances to update.
        Returns:
            None
        """
        worksheet = cls.get_worksheet(
            sheet_id=sheet_id,
            sheet_name=sheet_name,
        )
        mapping_dict = cls.updated_mapping_fields()
        update_batch = []

        for object in list_object:
            model_dict = object.model_dump(mode="json")

            for k, v in mapping_dict.items():
                update_batch.append(
                    {
                        "range": f"{v}{object.index}",
                        "values": [[model_dict[k]]],
                    }
                )

        if len(list_object) > 0:
            worksheet.batch_update(
                update_batch, value_input_option=ValueInputOption.user_entered
            )

    @retry_on_fail(max_retries=3, sleep_interval=30)
    def update(
        self,
    ) -> None:
        """
        Update the current row in the sheet with the model's data.
        Returns:
            None
        """
        mapping_dict = self.updated_mapping_fields()
        model_dict = self.model_dump(mode="json")

        worksheet = self.get_worksheet(
            sheet_id=self.sheet_id, sheet_name=self.sheet_name
        )

        update_batch = []
        for k, v in mapping_dict.items():
            update_batch.append(
                {
                    "range": f"{v}{self.index}",
                    "values": [[model_dict[k]]],
                }
            )

        worksheet.batch_update(
            update_batch, value_input_option=ValueInputOption.user_entered
        )

    @classmethod
    @retry_on_fail(max_retries=5, sleep_interval=30)
    def update_note_message(
        cls,
        sheet_id: str,
        sheet_name: str,
        index: int,
        messages: str,
    ):
        """
        Update the note message for a specific row in the sheet.
        Args:
            sheet_id (str): The ID of the Google Sheet.
            sheet_name (str): The name of the worksheet.
            index (int): The row index to update.
            messages (str): The message to write.
        Raises:
            SheetError: If the note message cannot be updated.
        """
        for field_name, field_info in cls.model_fields.items():
            if hasattr(field_info, "metadata"):
                for metadata in field_info.metadata:
                    if COL_META in metadata and IS_NOTE_META in metadata:
                        worksheet = cls.get_worksheet(
                            sheet_id=sheet_id,
                            sheet_name=sheet_name,
                        )

                        worksheet.batch_update(
                            [
                                {
                                    "range": f"{metadata[COL_META]}{index}",
                                    "values": [[messages]],
                                }
                            ],
                            value_input_option=ValueInputOption.user_entered,
                        )
                        return

        raise SheetError("Can't update sheet message")

    @classmethod
    @retry_on_fail(max_retries=5, sleep_interval=30)
    def batch_update_note_message(
        cls,
        sheet_id: str,
        sheet_name: str,
        update_payloads: list[NoteMessageUpdatePayload],
    ):
        """
        Batch update note messages for multiple rows in the sheet.
        Args:
            sheet_id (str): The ID of the Google Sheet.
            sheet_name (str): The name of the worksheet.
            update_payloads (list[NoteMessageUpdatePayload]): List of payloads containing row indexes and messages.
        Raises:
            SheetError: If the note messages cannot be updated.
        """
        for field_name, field_info in cls.model_fields.items():
            if hasattr(field_info, "metadata"):
                for metadata in field_info.metadata:
                    if (
                        COL_META in metadata
                        and IS_NOTE_META in metadata
                        and metadata[IS_NOTE_META]
                    ):
                        worksheet = cls.get_worksheet(
                            sheet_id=sheet_id,
                            sheet_name=sheet_name,
                        )

                        batch: list[dict] = []
                        for payload in update_payloads:
                            batch.append(
                                {
                                    "range": f"{metadata[COL_META]}{payload.index}",
                                    "values": [[payload.message]],
                                }
                            )
                        worksheet.batch_update(
                            batch, value_input_option=ValueInputOption.user_entered
                        )
                        return

        raise SheetError("Can't update sheet message")

    @classmethod
    @retry_on_fail(max_retries=5, sleep_interval=30)
    def free_style_batch_update(
        cls,
        sheet_id: str,
        sheet_name: str,
        update_payloads: list[BatchCellUpdatePayload],
    ):
        """
        Batch update arbitrary cells in the sheet with provided values.
        Args:
            sheet_id (str): The ID of the Google Sheet.
            sheet_name (str): The name of the worksheet.
            update_payloads (list[BatchCellUpdatePayload]): List of payloads specifying cell and value.
        """
        worksheet = cls.get_worksheet(sheet_id=sheet_id, sheet_name=sheet_name)
        batch: list[dict] = []
        for payload in update_payloads:
            batch.append(
                {
                    "range": payload.cell,
                    "values": [[payload.value]],
                }
            )

        worksheet.batch_update(batch, value_input_option=ValueInputOption.user_entered)

    @classmethod
    @retry_on_fail(max_retries=5, sleep_interval=10)
    def get_cell_value(
        cls,
        sheet_id: str,
        sheet_name: str,
        cell: str,
    ) -> Any | None:
        """
        Retrieve the value of a specific cell from the sheet.
        Args:
            sheet_id (str): The ID of the Google Sheet.
            sheet_name (str): The name of the worksheet.
            cell (str): The cell reference (e.g., 'A1').
        Returns:
            Any | None: The value of the cell, or None if not found.
        """
        res = gsheet_client.http_client.values_get(
            params={"valueRenderOption": "UNFORMATTED_VALUE"},
            id=sheet_id,
            range=f"{sheet_name}!{cell}",
        )

        stock = res.get("values", None)
        if stock:
            return stock[0][0]

        return None


class RowModel(ColSheetModel):
    CHECK: Annotated[
        str,
        {
            COL_META: "B",
        },
    ]
    PRODUCT_NAME: Annotated[
        str | None,
        {
            COL_META: "C",
        },
    ] = None
    PACK: Annotated[
        str | None,
        {
            COL_META: "D",
        },
    ] = None
    PRODUCT: Annotated[
        str | None,
        {
            COL_META: "E",
            IS_UPDATE_META: True,
        },
    ] = None
    CODE_ID: Annotated[
        int,
        {
            COL_META: "F",
        },
    ]
    REGIONS: Annotated[
        str | None,
        {
            COL_META: "G",
            IS_UPDATE_META: True,
        },
    ] = None
    DENOMINATION: Annotated[
        str | None,
        {
            COL_META: "H",
        },
    ] = None
    DISCOUNT: Annotated[
        str | None,
        {
            COL_META: "I",
        },
    ] = None
    PRICE_USD: Annotated[
        str | None,
        {
            COL_META: "J",
            IS_UPDATE_META: True,
        },
    ] = None
    PRICE: Annotated[
        str | None,
        {
            COL_META: "K",
            IS_UPDATE_META: True,
        },
    ] = None
    CURRENCY: Annotated[
        str | None,
        {
            COL_META: "L",
            IS_UPDATE_META: True,
        },
    ] = None
    NOTE: Annotated[
        str | None,
        {
            COL_META: "M",
            IS_UPDATE_META: True,
            IS_NOTE_META: True,
        },
    ] = None
    FILL_IN: Annotated[
        str | None,
        {
            COL_META: "N",
        },
    ] = None
    ID_SHEET: Annotated[
        str | None,
        {
            COL_META: "O",
        },
    ] = None
    SHEET: Annotated[
        str | None,
        {
            COL_META: "P",
        },
    ] = None
    RANGE_NOTE: Annotated[
        str | None,
        {
            COL_META: "Q",
        },
    ] = None
    CODE: Annotated[
        str | None,
        {
            COL_META: "R",
        },
    ] = None
    RANGE_CODE: Annotated[
        str | None,
        {
            COL_META: "S",
        },
    ] = None

    @classmethod
    @retry_on_fail(max_retries=5, sleep_interval=10)
    def get_run_indexes(
        cls, sheet_id: str, sheet_name: str, col_index: int
    ) -> list[int]:
        sheet = cls.get_worksheet(sheet_id=sheet_id, sheet_name=sheet_name)
        run_indexes = []
        check_col = sheet.col_values(col_index)
        for idx, value in enumerate(check_col):
            idx += 1
            if not isinstance(value, str):
                value = str(value)
            if value in [type.value for type in CheckType]:
                run_indexes.append(idx)

        return run_indexes
