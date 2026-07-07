from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker

from models import Customer
from utils import ensure_parent_dir, generate_id


class CustomerGenerator:
    """Generate realistic customer records and persist them to CSV."""

    def __init__(self, total_customers: int, seed: int, output_path: Path) -> None:
        self.total_customers = total_customers
        self.seed = seed
        self.output_path = output_path
        self.fake = Faker("en_IN")
        self.random = random.Random(seed)
        self.np_rng = np.random.default_rng(seed)

    def generate(self) -> list[Customer]:
        customers: list[Customer] = []
        for index in range(1, self.total_customers + 1):
            customer = self._generate_customer(index)
            customers.append(customer)

        df = pd.DataFrame([customer.to_dict() for customer in customers])
        ensure_parent_dir(self.output_path)
        df.to_csv(self.output_path, index=False)
        return customers

    def _generate_customer(self, index: int) -> Customer:
        gender = self.random.choice(["Male", "Female", "Other"])
        first_name = self.fake.first_name_male() if gender == "Male" else self.fake.first_name_female()
        last_name = self.fake.last_name()
        city, state, country = self._pick_location(index)
        segment = self._pick_segment(index)
        annual_income = self._pick_income(segment)
        preferred_min, preferred_max = self._pick_spend_range(segment, annual_income)
        account_open_date = self.fake.date_between(start_date="-8y", end_date="today").isoformat()
        risk_score = min(100, max(5, int(self.np_rng.normal(loc=35, scale=18))))
        card_number = self._generate_card_number()
        card_type = self.random.choice(["Visa", "Mastercard", "RuPay", "Amex"])
        customer_id = generate_id("CUST", index, width=6)
        trusted_device_id = generate_id("DEV", index, width=4)

        return Customer(
            customer_id=customer_id,
            first_name=first_name,
            last_name=last_name,
            gender=gender,
            age=max(18, min(75, int(self.fake.random_int(min=18, max=75)))),
            city=city,
            state=state,
            country=country,
            annual_income=round(annual_income, 2),
            customer_segment=segment,
            account_open_date=account_open_date,
            risk_score=risk_score,
            preferred_spending_min=round(preferred_min, 2),
            preferred_spending_max=round(preferred_max, 2),
            preferred_city=city,
            preferred_country=country,
            trusted_device_id=trusted_device_id,
            card_number=card_number,
            card_type=card_type,
        )

    def _pick_location(self, index: int) -> tuple[str, str, str]:
        india_cities = [
            ("Delhi", "Delhi", "India"),
            ("Mumbai", "Maharashtra", "India"),
            ("Bengaluru", "Karnataka", "India"),
            ("Hyderabad", "Telangana", "India"),
            ("Chennai", "Tamil Nadu", "India"),
            ("Kolkata", "West Bengal", "India"),
            ("Pune", "Maharashtra", "India"),
            ("Ahmedabad", "Gujarat", "India"),
        ]
        if index % 10 == 0:
            international_locations = [
                ("London", "England", "United Kingdom"),
                ("Dubai", "Dubai", "UAE"),
                ("Singapore", "Singapore", "Singapore"),
                ("New York", "New York", "United States"),
            ]
            return self.random.choice(international_locations)
        return self.random.choice(india_cities)

    def _pick_segment(self, index: int) -> str:
        choices = ["Regular", "Gold", "Platinum", "Corporate"]
        weights = [0.60, 0.20, 0.10, 0.10]
        return self.random.choices(choices, weights=weights, k=1)[0]

    def _pick_income(self, segment: str) -> float:
        mapping = {
            "Regular": (250000, 900000),
            "Gold": (900000, 2500000),
            "Platinum": (2500000, 8000000),
            "Corporate": (1500000, 5000000),
        }
        low, high = mapping[segment]
        return self.random.uniform(low, high)

    def _pick_spend_range(self, segment: str, annual_income: float) -> tuple[float, float]:
        if segment == "Regular":
            return annual_income * 0.02, annual_income * 0.08
        if segment == "Gold":
            return annual_income * 0.05, annual_income * 0.15
        if segment == "Platinum":
            return annual_income * 0.08, annual_income * 0.22
        return annual_income * 0.1, annual_income * 0.3

    def _generate_card_number(self) -> str:
        digits = [self.random.randint(0, 9) for _ in range(15)]
        digits.insert(0, self.random.randint(4, 5))
        digits = digits[:15]
        total = 0
        parity = len(digits) % 2
        for index, digit in enumerate(digits):
            value = digit
            if index % 2 == parity:
                value *= 2
                if value > 9:
                    value -= 9
            total += value
        check_digit = (10 - (total % 10)) % 10
        return "".join(str(d) for d in digits) + str(check_digit)
