from ..shared.paths import ROOT_PATH
from app import config

from gspread import service_account

gsheet_client = service_account(ROOT_PATH.joinpath(config.KEYS_PATH))
