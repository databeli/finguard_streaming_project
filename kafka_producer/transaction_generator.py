from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from fraud_engine import FraudEngine
from models import Customer, Merchant, Transaction
from utils import generate_id, utc_now_iso


class TransactionGenerator:
    """Generate realistic, profile-driven credit card transactions."""

    def __init__(
        self,
        customers: list[Customer],
        merchants: list[Merchant],
        fraud_engine: FraudEngine,
        random_seed: int,
        transactions_per_second: int,
    ) -> None:
        self.customers = customers
        self.merchants = merchants
        self.fraud_engine = fraud_engine
        self.random = random.Random(random_seed)
        self.np_rng = np.random.default_rng(random_seed)
        self.transactions_per_second = transactions_per_second
        self.transaction_counter = 0

    def generate_transaction(self) -> Transaction:
        customer = self._select_customer()
        merchant = self._select_merchant(customer)
        self.transaction_counter += 1
        transaction_id = generate_id("TXN", self.transaction_counter, width=7)
        amount = self._generate_amount(customer, merchant)
        is_international = self._is_international_transaction(customer, merchant)
        currency = self._choose_currency(is_international)
        transaction_type = self._choose_transaction_type(merchant)
        payment_channel = self._choose_payment_channel(transaction_type)
        device_id = self._choose_device_id(customer, merchant)
        transaction_timestamp = utc_now_iso()

        transaction = Transaction(
            transaction_id=transaction_id,
            customer_id=customer.customer_id,
            card_number=customer.card_number,
            merchant_id=merchant.merchant_id,
            merchant_name=merchant.merchant_name,
            merchant_category=merchant.merchant_category,
            amount=round(amount, 2),
            currency=currency,
            transaction_type=transaction_type,
            payment_channel=payment_channel,
            device_id=device_id,
            city=merchant.city,
            country=merchant.country,
            transaction_timestamp=transaction_timestamp,
            is_international=is_international,
            fraud_score=0,
            fraud_reason="NORMAL",
        )

        force_fraud = self.fraud_engine.should_force_fraud()
        fraud_score, fraud_reason = self.fraud_engine.evaluate(
            transaction=transaction,
            customer=customer,
            merchant=merchant,
            force_fraud=force_fraud,
        )
        transaction.fraud_score = fraud_score
        transaction.fraud_reason = fraud_reason
        return transaction

    def _generate_amount(self, customer: Customer, merchant: Merchant) -> float:
        min_spend = customer.preferred_spending_min
        max_spend = customer.preferred_spending_max
        if merchant.merchant_category in {"Hotel", "Airline", "Travel"}:
            amount = self.np_rng.uniform(max_spend * 0.6, max_spend * 1.8)
        elif merchant.merchant_category in {"Electronics", "Jewellery", "Shopping"}:
            amount = self.np_rng.uniform(min_spend * 0.8, max_spend * 1.2)
        elif merchant.merchant_category == "ATM":
            amount = self.np_rng.uniform(500, 5000)
        else:
            amount = self.np_rng.normal(loc=(min_spend + max_spend) / 2, scale=(max_spend - min_spend) / 5)
            amount = max(50, min(amount, max_spend * 2))
        return float(round(amount, 2))

    def _select_customer(self) -> Customer:
        return self.random.choice(self.customers)

    def _select_merchant(self, customer: Customer) -> Merchant:
        category_preferences = {
            "Regular": ["Grocery", "Restaurant", "Pharmacy", "Shopping"],
            "Gold": ["Restaurant", "Shopping", "Entertainment", "Travel"],
            "Platinum": ["Hotel", "Airline", "Jewellery", "Electronics"],
            "Corporate": ["Fuel", "Travel", "Restaurant", "Hotel"],
        }
        preferred_categories = category_preferences.get(customer.customer_segment, ["Shopping"])
        merchants_in_pref = [merchant for merchant in self.merchants if merchant.merchant_category in preferred_categories]
        return self.random.choice(merchants_in_pref)

    def _is_international_transaction(self, customer: Customer, merchant: Merchant) -> bool:
        travel_flag = customer.country != merchant.country and self.random.random() < 0.3
        return travel_flag or merchant.country != customer.preferred_country

    def _choose_currency(self, is_international: bool) -> str:
        if is_international and self.random.random() < 0.5:
            return self.random.choice(["USD", "EUR"])
        return "INR"

    def _choose_transaction_type(self, merchant: Merchant) -> str:
        if merchant.merchant_category == "ATM":
            return "ATM"
        if merchant.merchant_category in {"Shopping", "Electronics", "Grocery", "Pharmacy", "Restaurant"}:
            return self.random.choice(["PURCHASE", "ONLINE"])
        return "PURCHASE"

    def _choose_payment_channel(self, transaction_type: str) -> str:
        if transaction_type == "ATM":
            return "ATM"
        return self.random.choice(["POS", "MOBILE_APP", "INTERNET_BANKING"])

    def _choose_device_id(self, customer: Customer, merchant: Merchant) -> str:
        if self.random.random() < 0.15:
            return f"DEV{self.random.randint(1000, 9999)}"
        return customer.trusted_device_id
