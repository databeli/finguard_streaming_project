from __future__ import annotations

import json
import logging
import sys
from typing import Any

from confluent_kafka import Consumer, KafkaException

from config import load_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_consumer(settings) -> Consumer:
    """Build a Kafka consumer configured for Confluent Cloud."""
    config = {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": settings.api_key,
        "sasl.password": settings.api_secret,
        "group.id": "credit-card-simulator-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    return Consumer(config)


def main() -> None:
    settings = load_settings()
    consumer = create_consumer(settings)

    try:
        consumer.subscribe([settings.topic_name])
        logger.info("Listening for messages on topic=%s", settings.topic_name)

        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaException._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", message.error())
                continue

            payload = json.loads(message.value().decode("utf-8"))
            print(json.dumps(payload, indent=2, sort_keys=True))
            print("-" * 80)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
