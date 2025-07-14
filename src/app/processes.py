from typing import Final

from datetime import datetime

from gspread.worksheet import ValueRange
from gspread.utils import rowcol_to_a1

from .bbcp.api import bbcp_api_client
from .bbcp.models import CatalogResponse, FriProduct, ExchangeRates
from .sheet.models import BatchCellUpdatePayload, RowModel
from .sheet.enums import CheckType
from .shared.utils import split_list, sleep_for, formated_datetime
from .shared.decorators import retry_on_fail
from .sheet.utils import fri_a1_range_to_grid_range

from app import logger, config


RELAX_TIME_CELL: Final[str] = "Q2"


def to_product_dict(
    bbcp_catalog: CatalogResponse,
) -> dict[int, FriProduct]:
    product_dict: dict[int, FriProduct] = {}
    for brand in bbcp_catalog.brands:
        for product in brand.products:
            product_dict[product.id] = FriProduct(
                id=product.id,
                name=product.name,
                countryCode=brand.countryCode,
                price=product.price,
            )

    return product_dict


def to_exchange_rate_dict(
    exchange_rates: ExchangeRates,
) -> dict[str, float]:
    exchange_rate_dict: dict[str, float] = {}

    for rate in exchange_rates.rates:
        exchange_rate_dict[rate.currencyCode] = rate.value

    return exchange_rate_dict


def is_range_cell(range: str) -> bool:
    return ":" in range


def find_cell_to_update(row_models: list[RowModel]) -> dict[str, str]:
    mapping_dict: dict[str, str] = {}

    sheet_get_batch_dict: dict[str, dict[str, list[str]]] = {}

    for row_model in row_models:
        if (
            row_model.FILL_IN
            and row_model.ID_SHEET
            and row_model.SHEET
            and row_model.RANGE_NOTE
            and row_model.CODE
            and row_model.RANGE_CODE
        ):
            if row_model.ID_SHEET not in sheet_get_batch_dict:
                sheet_get_batch_dict[row_model.ID_SHEET] = {}
                sheet_get_batch_dict[row_model.ID_SHEET][row_model.SHEET] = [
                    row_model.RANGE_CODE
                ]
            else:
                if row_model.SHEET not in sheet_get_batch_dict[row_model.ID_SHEET]:
                    sheet_get_batch_dict[row_model.ID_SHEET][row_model.SHEET] = [
                        row_model.RANGE_CODE
                    ]
                else:
                    sheet_get_batch_dict[row_model.ID_SHEET][row_model.SHEET].append(
                        row_model.RANGE_CODE
                    )

    # dict[sheet_id, dict[sheet_name, range, value_range]]
    sheet_get_batch_result_dict: dict[str, dict[str, dict[str, ValueRange]]] = {}

    for sheet_id, sheet_names in sheet_get_batch_dict.items():
        for sheet_name, get_batch in sheet_names.items():
            _get_batch_resutl: list[ValueRange] = RowModel.get_worksheet(
                sheet_id=sheet_id, sheet_name=sheet_name
            ).batch_get(ranges=get_batch)
            for i, range in enumerate(_get_batch_resutl):
                if sheet_id not in sheet_get_batch_result_dict:
                    sheet_get_batch_result_dict[sheet_id] = {}
                if sheet_name not in sheet_get_batch_result_dict[sheet_id]:
                    sheet_get_batch_result_dict[sheet_id][sheet_name] = {}
                if (
                    get_batch[i]
                    not in sheet_get_batch_result_dict[sheet_id][sheet_name]
                ):
                    sheet_get_batch_result_dict[sheet_id][sheet_name][get_batch[i]] = (
                        range
                    )

    for row_model in row_models:
        if (
            row_model.ID_SHEET
            and row_model.SHEET
            and row_model.RANGE_NOTE
            and row_model.CODE
            and row_model.RANGE_CODE
        ):
            _codes_grid = sheet_get_batch_result_dict[row_model.ID_SHEET][
                row_model.SHEET
            ][row_model.RANGE_CODE]

            code_grid_range = fri_a1_range_to_grid_range(row_model.RANGE_CODE)
            note_grid_range = fri_a1_range_to_grid_range(row_model.RANGE_NOTE)
            for i, code_row in enumerate(_codes_grid):
                for j, code_col in enumerate(code_row):
                    if (
                        isinstance(code_col, str)
                        and row_model.CODE.strip() == code_col.strip()
                    ):
                        target_row_index = i + 1 + code_grid_range.startRowIndex
                        target_col_index = j + 1 + note_grid_range.startColumnIndex
                        mapping_dict[str(row_model.index)] = rowcol_to_a1(
                            target_row_index, target_col_index
                        )

    return mapping_dict


def batch_update_price(
    to_be_updated_row_models: list[RowModel],
):
    update_dict: dict[str, dict[str, list[BatchCellUpdatePayload]]] = {}

    need_to_find_cell_to_update: list[RowModel] = []

    for row_model in to_be_updated_row_models:
        if row_model.RANGE_NOTE and is_range_cell(row_model.RANGE_NOTE):
            need_to_find_cell_to_update.append(row_model)

    update_cell_mapping = find_cell_to_update(need_to_find_cell_to_update)

    for row_model in to_be_updated_row_models:
        if row_model.ID_SHEET and row_model.SHEET and row_model.RANGE_NOTE:
            target_update_cell: str | None = None
            if is_range_cell(row_model.RANGE_NOTE):
                if str(row_model.index) in update_cell_mapping:
                    target_update_cell = update_cell_mapping[str(row_model.index)]
            else:
                target_update_cell = row_model.RANGE_NOTE

            if target_update_cell:
                if row_model.ID_SHEET not in update_dict:
                    update_dict[row_model.ID_SHEET] = {}
                    update_dict[row_model.ID_SHEET][row_model.SHEET] = [
                        BatchCellUpdatePayload[str](
                            cell=target_update_cell,
                            value=row_model.PRICE_USD if row_model.PRICE_USD else "",
                        )
                    ]

                else:
                    if row_model.SHEET not in update_dict[row_model.ID_SHEET]:
                        update_dict[row_model.ID_SHEET][row_model.SHEET] = [
                            BatchCellUpdatePayload[str](
                                cell=target_update_cell,
                                value=row_model.PRICE_USD
                                if row_model.PRICE_USD
                                else "",
                            )
                        ]
                    else:
                        update_dict[row_model.ID_SHEET][row_model.SHEET].append(
                            BatchCellUpdatePayload[str](
                                cell=target_update_cell,
                                value=row_model.PRICE_USD
                                if row_model.PRICE_USD
                                else "",
                            )
                        )
    # print(update_dict)
    for sheet_id, sheet_names in update_dict.items():
        for sheet_name, update_batch in sheet_names.items():
            RowModel.free_style_batch_update(
                sheet_id=sheet_id, sheet_name=sheet_name, update_payloads=update_batch
            )


@retry_on_fail(max_retries=5, sleep_interval=10)
def batch_process(
    bbcp_product_dict: dict[int, FriProduct],
    exchange_rate_dict: dict[str, float],
    indexes: list[int],
):
    # Get all run row from sheet
    logger.info(f"Get all run row from sheet: {indexes}")
    row_models = RowModel.batch_get(
        sheet_id=config.SHEET_ID,
        sheet_name=config.SHEET_NAME,
        indexes=indexes,
    )

    to_be_updated_row_models: list[RowModel] = []

    # Process for each row model

    logger.info("Processing")
    for row_model in row_models:
        if row_model.CODE_ID in bbcp_product_dict:
            # Calculate Price USD
            __price_usd: float = bbcp_product_dict[row_model.CODE_ID].price.max
            if bbcp_product_dict[row_model.CODE_ID].price.currencyCode.upper() != "USD":
                if (
                    bbcp_product_dict[row_model.CODE_ID].price.currencyCode
                    in exchange_rate_dict
                ):
                    __price_usd = (
                        bbcp_product_dict[row_model.CODE_ID].price.max
                        * exchange_rate_dict[
                            bbcp_product_dict[row_model.CODE_ID].price.currencyCode
                        ]
                    )
                else:
                    __price_usd = 0

            row_model.PRODUCT = bbcp_product_dict[row_model.CODE_ID].name
            row_model.REGIONS = bbcp_product_dict[row_model.CODE_ID].countryCode
            row_model.PRICE_USD = str(__price_usd)
            row_model.PRICE = str(bbcp_product_dict[row_model.CODE_ID].price.max)
            row_model.CURRENCY = bbcp_product_dict[row_model.CODE_ID].price.currencyCode
            row_model.NOTE = f"{formated_datetime(datetime.now())} Cập nhật thành công"

            if row_model.FILL_IN == CheckType.RUN.value:
                to_be_updated_row_models.append(row_model)

        else:
            row_model.NOTE = f"{formated_datetime(datetime.now())} Không tìm thấy product với CODE_ID: {row_model.CODE_ID}"
            row_model.PRICE = ""
            row_model.PRICE_USD = ""
            if row_model.FILL_IN == CheckType.RUN.value:
                to_be_updated_row_models.append(row_model)

    logger.info("Price sheet updating")
    batch_update_price(to_be_updated_row_models)

    logger.info("Sheet updating")
    RowModel.batch_update(
        sheet_id=config.SHEET_ID,
        sheet_name=config.SHEET_NAME,
        list_object=row_models,
    )

    sleep_for(config.RELAX_TIME_EACH_BATCH)


def process():
    logger.info("# Getting Bamboo Card Portal Catalog")
    bbcp_catalog = bbcp_api_client.get_catalog()  # TODO: change to real

    bbcp_product_dict = to_product_dict(bbcp_catalog)

    logger.info(f"## Total product: {len(bbcp_product_dict)}")

    logger.info("## Getting Bammboo Card Portal Exchange rates")

    exchange_rates = bbcp_api_client.get_exchange_rates()

    exchange_rate_dict = to_exchange_rate_dict(exchange_rates)

    # Get run_indexes from sheet
    run_indexes = RowModel.get_run_indexes(
        sheet_id=config.SHEET_ID,
        sheet_name=config.SHEET_NAME,
        col_index=2,
    )

    for batch_indexes in split_list(run_indexes, config.PROCESS_BATCH_SIZE):
        batch_process(
            bbcp_product_dict=bbcp_product_dict,
            exchange_rate_dict=exchange_rate_dict,
            indexes=batch_indexes,
        )

    str_relax_time = RowModel.get_cell_value(
        sheet_id=config.SHEET_ID,
        sheet_name=config.SHEET_NAME,
        cell=RELAX_TIME_CELL,
    )

    sleep_for(float(str_relax_time) if str_relax_time else 10)
