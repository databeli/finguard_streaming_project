from __future__ import annotations

import json
import logging
import random
import signal
import time

from confluent_kafka import Producer

from config import load_settings
from customer_generator import CustomerGenerator
from merchant_generator import MerchantGenerator
from fraud_engine import FraudEngine
from transaction_generator import TransactionGenerator
from utils import validate_json_payload

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

## Update card number to a specific value for testing purposes
CARD_NUMBER = "5008514036965665"


def _build_payload(txn) -> dict:
    payload = txn.to_dict()
    payload.pop("fraud_score", None)
    payload.pop("fraud_reason", None)
    return payload


def _prepare_transaction(txn):
    txn.transaction_id = f"TXF{random.randint(100000, 999999)}"
    txn.amount = round(random.uniform(1.0, 99999.99), 2)
    txn.card_number = CARD_NUMBER
    return txn


def _create_producer(settings) -> Producer:
    config = {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": settings.api_key,
        "sasl.password": settings.api_secret,
        "client.id": "credit-card-stream-simulator-fraud-card",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "enable.idempotence": True,
    }
    return Producer(config)


def main() -> None:
    settings = load_settings()

    producer = _create_producer(settings)

    data_dir = settings.base_dir / "data"
    customers = CustomerGenerator(
        total_customers=settings.total_customers,
        seed=settings.random_seed,
        output_path=data_dir / "customers.csv",
    ).generate()
    merchants = MerchantGenerator(
        total_merchants=settings.total_merchants,
        seed=settings.random_seed,
        output_path=data_dir / "merchants.csv",
    ).generate()

    fraud_engine = FraudEngine(fraud_percentage=settings.fraud_percentage, seed=settings.random_seed)
    txn_gen = TransactionGenerator(
        customers=customers,
        merchants=merchants,
        fraud_engine=fraud_engine,
        random_seed=settings.random_seed,
        transactions_per_second=settings.transactions_per_second,
    )

    running = True

    def _handle_signal(sig, frame):
        nonlocal running
        logger.warning("Received signal %s. Stopping.", sig)
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        logger.info("Starting fraud-card producer for topic=%s", settings.topic_name)
        if not running:
            return

        txn = _prepare_transaction(txn_gen.generate_transaction())
        payload = _build_payload(txn)
        if not validate_json_payload(payload):
            logger.error("Invalid payload generated; skipping")
            return

        producer.produce(
            settings.topic_name,
            key=txn.transaction_id,
            value=json.dumps(payload),
        )
        producer.poll(0)
        logger.info("Produced %s | Amount=%.2f | Card=%s", txn.transaction_id, txn.amount, txn.card_number)

    except KeyboardInterrupt:
        logger.info("Interrupted by user; shutting down.")
    finally:
        logger.info("Flushing producer and exiting.")
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()
