import httpx

from typing import Final

from .models import CatalogResponse, ExchangeRates
from ..shared.decorators import retry_on_fail
from ..shared.paths import SRC_PATH
from ..shared.utils import sleep_for
from . import logger

from app import config

BBCP_BASE_URL: Final[str] = "https://api.bamboocardportal.com"


class BbcpAPIClient:
    def __init__(self) -> None:
        self.client = httpx.Client(
            auth=(config.BBCP_CLIENT_ID, config.BBCP_CLIENT_SECRET),
            timeout=60,
        )
        self.base_url = BBCP_BASE_URL

    def fake_get_catalog(self) -> CatalogResponse:
        import json

        with open(SRC_PATH / "data" / "catalogs.json") as f:
            data = json.load(f)
            return CatalogResponse.model_validate(data)

    def get_orders(self) -> dict:
        res = self.client.get(
            f"{self.base_url}/api/integration/v1/orders?startDate=2025-06-24&endDate=2025-06-25"
        )

        try:
            res.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.exception(e)
            logger.info(res.text)
            res.raise_for_status()

        return res.json()

    @retry_on_fail()
    def get_exchange_rates(self) -> ExchangeRates:
        res = self.client.get(f"{self.base_url}/api/integration/v1.0/exchange-rates")

        try:
            res.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.exception(e)
            logger.info(res.text)
            if res.status_code == 429:
                sleep_for(10)
            res.raise_for_status()

        return ExchangeRates.model_validate(res.json())

    @retry_on_fail(max_retries=60, sleep_interval=60)
    def get_catalog(self) -> CatalogResponse:
        res = self.client.get(f"{self.base_url}/api/integration/v1.0/catalog")

        try:
            res.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.exception(e)
            logger.info(res.text)
            if res.status_code == 429:
                sleep_for(10 * 60)
            res.raise_for_status()

        return CatalogResponse.model_validate(res.json())


bbcp_api_client = BbcpAPIClient()
