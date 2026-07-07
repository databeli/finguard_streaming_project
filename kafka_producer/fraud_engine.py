from __future__ import annotations

import random
from collections import deque
from datetime import datetime, timedelta
from typing import Deque

from models import Transaction


class FraudEngine:
    """Evaluate and score fraud risk for generated transactions."""

    def __init__(self, fraud_percentage: float, seed: int) -> None:
        self.fraud_percentage = fraud_percentage
        self.random = random.Random(seed)
        self.customer_history: dict[str, Deque[Transaction]] = {}
        self._reason_weights = {
            "HIGH_VALUE_TRANSACTION": 40,
            "IMPOSSIBLE_TRAVEL": 50,
            "NEW_DEVICE": 20,
            "HIGH_RISK_MERCHANT": 25,
            "BLACKLISTED_MERCHANT": 60,
            "INTERNATIONAL_TRANSACTION": 25,
            "VELOCITY_FRAUD": 45,
            "CARD_TESTING": 30,
        }

    def should_force_fraud(self) -> bool:
        return self.random.random() < self.fraud_percentage

    def evaluate(
        self,
        transaction: Transaction,
        customer: object,
        merchant: object,
        force_fraud: bool = False,
    ) -> tuple[int, str]:
        score = 0
        reasons: list[str] = []

        if transaction.amount > 100000:
            score += self._reason_weights["HIGH_VALUE_TRANSACTION"]
            reasons.append("HIGH_VALUE_TRANSACTION")

        previous = self._get_previous_transaction(transaction.customer_id)
        if (
            previous is not None
            and previous.city == "Delhi"
            and transaction.city == "London"
            and self._minutes_between(previous.transaction_timestamp, transaction.transaction_timestamp) <= 20
        ):
            score += self._reason_weights["IMPOSSIBLE_TRAVEL"]
            reasons.append("IMPOSSIBLE_TRAVEL")

        if transaction.device_id != customer.trusted_device_id:
            score += self._reason_weights["NEW_DEVICE"]
            reasons.append("NEW_DEVICE")

        if merchant.merchant_risk == "HIGH":
            score += self._reason_weights["HIGH_RISK_MERCHANT"]
            reasons.append("HIGH_RISK_MERCHANT")

        if merchant.is_blacklisted:
            score += self._reason_weights["BLACKLISTED_MERCHANT"]
            reasons.append("BLACKLISTED_MERCHANT")

        if transaction.is_international:
            score += self._reason_weights["INTERNATIONAL_TRANSACTION"]
            reasons.append("INTERNATIONAL_TRANSACTION")

        if self._is_velocity_fraud(transaction.customer_id):
            score += self._reason_weights["VELOCITY_FRAUD"]
            reasons.append("VELOCITY_FRAUD")

        if 5 <= transaction.amount <= 20 and self._is_card_testing(transaction.customer_id):
            score += self._reason_weights["CARD_TESTING"]
            reasons.append("CARD_TESTING")

        if force_fraud and not reasons:
            reason = self.random.choice(list(self._reason_weights.keys()))
            score += self._reason_weights[reason]
            reasons.append(reason)

        if force_fraud and reasons:
            # If we are forcing fraud, choose the highest-impact reason for the output.
            reasons = [max(reasons, key=lambda item: self._reason_weights[item])]
            score = min(100, max(score, self._reason_weights[reasons[0]]))

        score = min(100, score)
        reason = reasons[0] if reasons else "NORMAL"
        self._record_transaction(transaction)
        return score, reason

    def _record_transaction(self, transaction: Transaction) -> None:
        history = self.customer_history.setdefault(transaction.customer_id, deque(maxlen=50))
        history.append(transaction)

    def _get_previous_transaction(self, customer_id: str) -> Transaction | None:
        history = self.customer_history.get(customer_id)
        if not history:
            return None
        return history[-1]

    def _is_velocity_fraud(self, customer_id: str) -> bool:
        history = self.customer_history.get(customer_id, deque())
        cutoff = datetime.now().replace(tzinfo=None) - timedelta(seconds=30)
        recent = [txn for txn in history if self._parse_timestamp(txn.transaction_timestamp) >= cutoff]
        return len(recent) >= 5

    def _is_card_testing(self, customer_id: str) -> bool:
        history = self.customer_history.get(customer_id, deque())
        cutoff = datetime.now().replace(tzinfo=None) - timedelta(seconds=60)
        recent = [txn for txn in history if self._parse_timestamp(txn.transaction_timestamp) >= cutoff]
        return len(recent) >= 3

    def _minutes_between(self, first: str, second: str) -> float:
        first_dt = self._parse_timestamp(first)
        second_dt = self._parse_timestamp(second)
        return abs((second_dt - first_dt).total_seconds()) / 60

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
