import os

from dotenv import load_dotenv

from pydantic import BaseModel


class Config(BaseModel):
    # Keys
    KEYS_PATH: str

    # Sheets
    SHEET_ID: str
    SHEET_NAME: str

    BBCP_CLIENT_ID: str
    BBCP_CLIENT_SECRET: str

    PROCESS_BATCH_SIZE: int

    RELAX_TIME_EACH_BATCH: float

    RELAX_TIME_EACH_ROUND: float

    @staticmethod
    def from_env(dotenv_path: str = "settings.env") -> "Config":
        load_dotenv(dotenv_path)
        return Config.model_validate(os.environ)
