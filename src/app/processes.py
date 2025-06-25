from typing import Final

from datetime import datetime

from .bbcp.api import bbcp_api_client
from .bbcp.models import CatalogResponse, FriProduct
from .sheet.models import BatchCellUpdatePayload, RowModel
from .sheet.enums import CheckType
from .shared.utils import split_list, sleep_for, formated_datetime
from .shared.decorators import retry_on_fail

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


def batch_update_price(
    to_be_updated_row_models: list[RowModel],
):
    update_dict: dict[str, dict[str, list[BatchCellUpdatePayload]]] = {}
    for row_model in to_be_updated_row_models:
        if row_model.ID_SHEET and row_model.SHEET and row_model.CELL:
            if row_model.ID_SHEET not in update_dict:
                update_dict[row_model.ID_SHEET] = {}
                update_dict[row_model.ID_SHEET][row_model.SHEET] = [
                    BatchCellUpdatePayload[str](
                        cell=row_model.CELL,
                        value=row_model.PRICE if row_model.PRICE else "",
                    )
                ]

            else:
                if row_model.SHEET not in update_dict[row_model.ID_SHEET]:
                    update_dict[row_model.ID_SHEET][row_model.SHEET] = [
                        BatchCellUpdatePayload[str](
                            cell=row_model.CELL,
                            value=row_model.PRICE if row_model.PRICE else "",
                        )
                    ]
                else:
                    update_dict[row_model.ID_SHEET][row_model.SHEET].append(
                        BatchCellUpdatePayload[str](
                            cell=row_model.CELL,
                            value=row_model.PRICE if row_model.PRICE else "",
                        )
                    )

    for sheet_id, sheet_names in update_dict.items():
        for sheet_name, update_batch in sheet_names.items():
            RowModel.free_style_batch_update(
                sheet_id=sheet_id, sheet_name=sheet_name, update_payloads=update_batch
            )


@retry_on_fail(max_retries=5, sleep_interval=10)
def batch_process(
    bbcp_product_dict: dict[int, FriProduct],
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
            row_model.PRODUCT = bbcp_product_dict[row_model.CODE_ID].name
            row_model.REGIONS = bbcp_product_dict[row_model.CODE_ID].countryCode
            row_model.PRICE = str(bbcp_product_dict[row_model.CODE_ID].price.max)
            row_model.CURRENCY = bbcp_product_dict[row_model.CODE_ID].price.currencyCode
            row_model.NOTE = f"{formated_datetime(datetime.now())} Cập nhật thành công"

            if row_model.FILL_IN == CheckType.RUN.value:
                to_be_updated_row_models.append(row_model)

        else:
            row_model.NOTE = f"{formated_datetime(datetime.now())} Không tìm thấy product với CODE_ID: {row_model.CODE_ID}"
            row_model.PRICE = ""
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

    # Get run_indexes from sheet
    run_indexes = RowModel.get_run_indexes(
        sheet_id=config.SHEET_ID,
        sheet_name=config.SHEET_NAME,
        col_index=2,
    )

    for batch_indexes in split_list(run_indexes, config.PROCESS_BATCH_SIZE):
        batch_process(
            bbcp_product_dict=bbcp_product_dict,
            indexes=batch_indexes,
        )

    str_relax_time = RowModel.get_cell_value(
        sheet_id=config.SHEET_ID,
        sheet_name=config.SHEET_NAME,
        cell=RELAX_TIME_CELL,
    )

    sleep_for(float(str_relax_time) if str_relax_time else 10)
