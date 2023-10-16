from typing import Optional
from pydantic import BaseSettings, validator
from pathlib import Path
import os
from os import environ


class Settings(BaseSettings):
    LOG_LEVEL: str = 'DEBUG'
    LIMIT: int = 500
    LOCAL_SAVE_DIRECTORY: str = ''
    WRITE_FILE_LOCATION: str = 's3'  # local
    WRITE_FILE_FORMAT: str = 'csv'  # parquet, json
    OPEN_DATA_BUCKET: str = 'openaq-open-data-testing'
    DB_BACKUP_BUCKET: str = 'openaq-db-backups'
    DATABASE_READ_USER: str = 'postgres'
    DATABASE_READ_PASSWORD: str = 'postgres'
    DATABASE_WRITE_USER: str = 'postgres'
    DATABASE_WRITE_PASSWORD: str = 'postgres'
    DATABASE_HOST: str = 'localhost'
    DATABASE_PORT: int = 5432
    DATABASE_DB: str = 'postgres'
    DATABASE_READ_URL: Optional[str]
    DATABASE_WRITE_URL: Optional[str]
    TESTLOCAL: bool = True


    @validator('DATABASE_READ_URL', allow_reuse=True)
    def get_read_url(cls, v, values):
        return v or f"postgresql://{values['DATABASE_READ_USER']}:{values['DATABASE_READ_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"

    @validator('DATABASE_WRITE_URL', allow_reuse=True)
    def get_write_url(cls, v, values):
        return v or f"postgresql://{values['DATABASE_WRITE_USER']}:{values['DATABASE_WRITE_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"

    class Config:
        try:
            parent = Path(__file__).parents[2]
            # env_path = Path(__file__).resolve().parent
        except Exception:
            # If we are not running as a script
            parent = Path(os.getcwd())

        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        elif 'ENV' in environ:
            env_file = Path.joinpath(parent, f".env.{environ['ENV']}")
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
