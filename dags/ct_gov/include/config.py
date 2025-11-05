from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
load_dotenv()

columns_to_read = ["studies.protocolSection"]

class Settings(BaseSettings):
    BASE_URL: str
    FIRST_PAGE_URL: str

    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str
    CTGOV_STAGING_BUCKET: str

    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_CONN_STR: str
    COLUMNS_TO_READ: List  = columns_to_read


    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

config = Settings()
