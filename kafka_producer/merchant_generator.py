from __future__ import annotations

import random
from pathlib import Path

import pandas as pd
from faker import Faker

from models import Merchant
from utils import ensure_parent_dir, generate_id


class MerchantGenerator:
    """Generate realistic merchant records and persist them to CSV."""

    def __init__(self, total_merchants: int, seed: int, output_path: Path) -> None:
        self.total_merchants = total_merchants
        self.seed = seed
        self.output_path = output_path
        self.fake = Faker("en_IN")
        self.random = random.Random(seed)

    def generate(self) -> list[Merchant]:
        merchants: list[Merchant] = []
        categories = [
            "Grocery",
            "Fuel",
            "Restaurant",
            "Hotel",
            "Electronics",
            "Jewellery",
            "ATM",
            "Pharmacy",
            "Shopping",
            "Airline",
            "Entertainment",
            "Travel",
        ]
        for index in range(1, self.total_merchants + 1):
            category = categories[index % len(categories)]
            merchant_name = self._build_merchant_name(category, index)
            city, country = self._pick_location(index)
            risk = self._pick_risk(index)
            merchants.append(
                Merchant(
                    merchant_id=generate_id("MER", index, width=4),
                    merchant_name=merchant_name,
                    merchant_category=category,
                    city=city,
                    country=country,
                    merchant_risk=risk,
                    is_blacklisted=self._is_blacklisted(index),
                )
            )

        df = pd.DataFrame([merchant.to_dict() for merchant in merchants])
        ensure_parent_dir(self.output_path)
        df.to_csv(self.output_path, index=False)
        return merchants

    def _build_merchant_name(self, category: str, index: int) -> str:
        name_templates = {
            "Grocery": ["FreshMart", "Daily Needs", "Grocer Go", "City Pantry"],
            "Fuel": ["FastFuel", "Highway Gas", "PetroPoint", "FuelX"],
            "Restaurant": ["Bistro Hub", "Tasty Table", "Spice Avenue", "Cafe Delight"],
            "Hotel": ["Grand Stay", "Comfort Inn", "Skyline Hotel", "Holiday Lodge"],
            "Electronics": ["TechWorld", "Digital Hub", "Gadget Plaza", "ElectroMart"],
            "Jewellery": ["Sparkle Gold", "Royal Gems", "Diamond Lane", "LuxJewels"],
            "ATM": ["Bank ATM", "CashPoint", "Quick Cash", "Money Access"],
            "Pharmacy": ["MediCare", "HealthFirst", "Wellness Plus", "Pharma Express"],
            "Shopping": ["ShopSphere", "MarketPlace", "Mall Mart", "Retail City"],
            "Airline": ["SkyFly", "Orbit Air", "AirMetro", "FlightLink"],
            "Entertainment": ["FunZone", "Cinema City", "PlayLand", "EventHub"],
            "Travel": ["TripVista", "Globe Trek", "TravelEase", "WanderGo"],
        }
        base = name_templates.get(category, ["Merchant"])
        return f"{self.random.choice(base)} {index}"

    def _pick_location(self, index: int) -> tuple[str, str]:
        india_cities = [
            ("Delhi", "India"),
            ("Mumbai", "India"),
            ("Bengaluru", "India"),
            ("Hyderabad", "India"),
            ("Chennai", "India"),
            ("Kolkata", "India"),
            ("Pune", "India"),
            ("Ahmedabad", "India"),
            ("Jaipur", "India"),
            ("Lucknow", "India"),
        ]
        if index % 15 == 0:
            international = [
                ("London", "United Kingdom"),
                ("Dubai", "UAE"),
                ("Singapore", "Singapore"),
                ("New York", "United States"),
            ]
            return self.random.choice(international)
        return self.random.choice(india_cities)

    def _pick_risk(self, index: int) -> str:
        weights = {
            "LOW": 0.65,
            "MEDIUM": 0.25,
            "HIGH": 0.10,
        }
        return self.random.choices(
            list(weights.keys()),
            weights=list(weights.values()),
            k=1,
        )[0]

    def _is_blacklisted(self, index: int) -> bool:
        return index % 20 == 0
