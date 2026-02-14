# src/config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()  # reads .env locally
load_dotenv(".env.docker") # try .env.docker if .env is missing


# dataclass is built in python module that helps you create class without the boilerplate
# code like __init__ and __repr__

# frozen=True makes the class immutable
@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

def get_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        db=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )