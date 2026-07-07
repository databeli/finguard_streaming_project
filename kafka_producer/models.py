from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class Customer:
    customer_id: str
    first_name: str
    last_name: str
    gender: str
    age: int
    city: str
    state: str
    country: str
    annual_income: float
    customer_segment: str
    account_open_date: str
    risk_score: float
    preferred_spending_min: float
    preferred_spending_max: float
    preferred_city: str
    preferred_country: str
    trusted_device_id: str
    card_number: str
    card_type: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Merchant:
    merchant_id: str
    merchant_name: str
    merchant_category: str
    city: str
    country: str
    merchant_risk: str
    is_blacklisted: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Transaction:
    transaction_id: str
    customer_id: str
    card_number: str
    merchant_id: str
    merchant_name: str
    merchant_category: str
    amount: float
    currency: str
    transaction_type: str
    payment_channel: str
    device_id: str
    city: str
    country: str
    transaction_timestamp: str
    is_international: bool
    fraud_score: int
    fraud_reason: str
    status: str = "APPROVED"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["amount"] = round(data["amount"], 2)
        return data


@dataclass(slots=True)
class StatsSnapshot:
    transactions_produced: int
    average_tps: float
    fraud_percentage: float
    runtime_seconds: float
