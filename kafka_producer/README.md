# Credit Card Transaction Simulator

A production-oriented Python simulator that generates realistic credit card transactions and publishes them to a Kafka topic for downstream fraud detection workflows.

## Features

- Generates realistic customer and merchant datasets
- Simulates spending behavior that is consistent per customer
- Applies configurable fraud rules and scores
- Publishes JSON transactions to Confluent Cloud Kafka
- Logs delivery status, transaction metadata, and runtime stats
- Handles keyboard interrupts and graceful shutdown

## Installation

### 1. Create a virtual environment

```bash
python -m venv .venv
```

### 2. Activate the environment

On Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

On macOS/Linux:

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

## Environment Configuration

Create a `.env` file from the example:

```bash
copy .env.example .env
```

Update the values for your Kafka cluster:

```env
BOOTSTRAP_SERVERS=your-bootstrap-servers:9092
API_KEY=your_api_key
API_SECRET=your_api_secret
TOPIC_NAME=credit_card_transactions
TRANSACTIONS_PER_SECOND=5
FRAUD_PERCENTAGE=0.08
TOTAL_CUSTOMERS=1000
TOTAL_MERCHANTS=200
RANDOM_SEED=42
```

## Running the Producer

```bash
python producer.py
```

The simulator will:

1. Generate customer and merchant datasets if they do not already exist.
2. Start producing transactions to the configured Kafka topic.
3. Print periodic statistics every 100 messages.

## Example Output

```text
INFO - TXN000001 | Amount=1450.50 | Merchant=Amazon India | Fraud=15 | Reason=NORMAL | Offset=None
INFO - TXN000002 | Amount=9500.00 | Merchant=Delhi Grocers | Fraud=45 | Reason=VELOCITY_FRAUD | Offset=12
```

## Project Architecture

- [config.py](config.py) loads configuration values from `.env`
- [models.py](models.py) defines dataclasses for customers, merchants, transactions, and stats
- [customer_generator.py](customer_generator.py) creates realistic customer records
- [merchant_generator.py](merchant_generator.py) creates merchant records
- [transaction_generator.py](transaction_generator.py) simulates transaction behavior
- [fraud_engine.py](fraud_engine.py) evaluates fraud conditions and scores
- [producer.py](producer.py) publishes messages to Kafka and handles runtime control
- [utils.py](utils.py) contains shared helpers

## Folder Explanation

- `data/` stores generated customer and merchant CSV files
- `.env` contains runtime secrets and Kafka configuration
- `requirements.txt` lists project dependencies

## Future Enhancements

- Add schema validation using JSON Schema or Pydantic
- Support multiple Kafka topics for fraud vs. clean traffic
- Persist transaction history for replay and testing
- Add Prometheus metrics for monitoring
