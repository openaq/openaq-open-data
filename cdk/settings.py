from pydantic import BaseSettings
from pathlib import Path
from os import environ, path
import sys

p = path.abspath('../lambda')
sys.path.insert(1, p)

from open_data_export.config import settings

class Settings(BaseSettings):
    ENV: str = "staging"
    PROJECT: str = "openaq"
    INGEST_LAMBDA_TIMEOUT: int = 900
    INGEST_LAMBDA_MEMORY_SIZE: int = 1536
    OPEN_DATA_ROLE_ARN: str = None
    VPC_ID: str = None
    ENV_VARIABLES: dict = settings

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        else:
            env_file = Path.joinpath(parent, ".env")
        print(env_file)

settings = Settings()
