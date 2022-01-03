from typing import Optional
from pydantic import BaseSettings, validator
from pathlib import Path

try:
    env_path = Path(__file__).resolve().parent
except Exception:
    # If we are not running as a script
    env_path = Path(os.getcwd())

env_file = Path.joinpath(env_path, ".env")


class Settings(BaseSettings):
    LOG_LEVEL: str = 'DEBUG'
    WRITE_FILE_FORMAT: str = 'csv'
    OPEN_DATA_BUCKET: str = ''
    DATABASE_READ_USER: str# = 'apiuser'
    DATABASE_READ_PASSWORD: str# = 'apiuserpw'
    DATABASE_WRITE_USER: str# = 'postgres'
    DATABASE_WRITE_PASSWORD: str# = 'postgres'
    DATABASE_HOST: str #= '192.168.192.2'
    DATABASE_PORT: int# = 5432
    DATABASE_DB: str# = 'toer'
    DATABASE_READ_URL: Optional[str]
    DATABASE_WRITE_URL: Optional[str]
    TESTLOCAL: bool = True

    @validator('DATABASE_READ_URL', allow_reuse=True)
    def get_read_url(cls, v, values):
        return v or f"postgresql://{values['DATABASE_READ_USER']}:{values['DATABASE_READ_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"

    class Config:
        env_file = env_file


settings = Settings()
