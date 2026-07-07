from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    api_key: str
    api_secret: str
    topic_name: str
    transactions_per_second: int
    fraud_percentage: float
    total_customers: int
    total_merchants: int
    random_seed: int
    base_dir: Path


def load_settings(base_dir: Path | str | None = None) -> Settings:
    """Load application settings from the environment and `.env` file."""
    root = Path(base_dir) if base_dir else Path(__file__).resolve().parent
    env_path = root / ".env"
    load_dotenv(env_path, override=False)

    settings = Settings(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS", "").strip(),
        api_key=os.getenv("API_KEY", "").strip(),
        api_secret=os.getenv("API_SECRET", "").strip(),
        topic_name=os.getenv("TOPIC_NAME", "credit_card_transactions").strip(),
        transactions_per_second=int(os.getenv("TRANSACTIONS_PER_SECOND", "5")),
        fraud_percentage=float(os.getenv("FRAUD_PERCENTAGE", "0.08")),
        total_customers=int(os.getenv("TOTAL_CUSTOMERS", "1000")),
        total_merchants=int(os.getenv("TOTAL_MERCHANTS", "200")),
        random_seed=int(os.getenv("RANDOM_SEED", "42")),
        base_dir=root,
    )
    validate_settings(settings)
    return settings


def validate_settings(settings: Settings) -> None:
    required = {
        "BOOTSTRAP_SERVERS": settings.bootstrap_servers,
        "API_KEY": settings.api_key,
        "API_SECRET": settings.api_secret,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    if settings.transactions_per_second <= 0:
        raise ValueError("TRANSACTIONS_PER_SECOND must be positive")
    if not 0 <= settings.fraud_percentage <= 1:
        raise ValueError("FRAUD_PERCENTAGE must be between 0 and 1")
    if settings.total_customers <= 0:
        raise ValueError("TOTAL_CUSTOMERS must be positive")
    if settings.total_merchants <= 0:
        raise ValueError("TOTAL_MERCHANTS must be positive")
