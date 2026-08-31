# Cryptocurrency Real-Time Streaming Analytics
## Assignment 2 — Sample Data & Kafka Producer

This project implements a Kafka-based cryptocurrency streaming ingestion layer for **Streaming Data Analytics — Assignment 2**.

It directly extends the architecture proposed in Assignment 1 by simulating realistic Binance-style cryptocurrency trade and candlestick events and publishing them to Apache Kafka.

---

## Project Context

**Industry:** Banking / Fintech

**Use Case:** Cryptocurrency Real-Time Market Analytics

### Assignment 1 Proposed Architecture

```text
Binance WebSocket
        ↓
      Kafka
        ↓
Python Consumers
        ↓
 MongoDB / SQL
        ↓
    Dashboard
```

Assignment 2 implements the Kafka ingestion stage using realistic sample cryptocurrency market data.

---

## Assignment 2 Objective

The objective of this assignment is to:

- Create realistic sample cryptocurrency data
- Configure Kafka topics
- Develop a Python Kafka producer
- Serialize streaming events as JSON
- Publish trade and kline events to Kafka
- Consume the events using a Python consumer
- Demonstrate continuous message flow
- Display Kafka topic, partition and offset metadata

---

## Architecture

```text
             Binance-Style Sample Data
                       |
              +--------+--------+
              |                 |
              v                 v
        Trade Events       Kline Events
              |                 |
              v                 v
        crypto_trades      crypto_klines
              |                 |
              +--------+--------+
                       |
                  Apache Kafka
                       |
                       v
                   consumer.py
                       |
                       v
                Terminal Output
```

---

## Project Structure

```text
Assignment_2_Sample_Data_Kafka_Producer/
│
├── data/
│   ├── sample_trades.json
│   └── sample_klines.json
│
├── screenshots/
│   └── kafka_stream_demo.png
│
├── producer.py
├── consumer.py
├── README.md
└── requirements.txt
```

---

## Cryptocurrency Trading Pairs

The sample dataset contains multiple cryptocurrency trading pairs:

- BTCUSDT
- ETHUSDT
- BNBUSDT
- SOLUSDT

Using multiple symbols makes the stream closer to a real cryptocurrency market-data pipeline.

---

## Trade Event Schema

Trade events are modeled after Binance-style trade data.

Example:

```json
{
  "event_type": "trade",
  "symbol": "BTCUSDT",
  "timestamp": "2026-08-31T10:00:01.000Z",
  "price": 112450.20,
  "quantity": 0.0042,
  "trade_id": 100001,
  "source": "binance_sample"
}
```

### Trade Fields

| Field | Description |
|---|---|
| event_type | Type of streaming event |
| symbol | Cryptocurrency trading pair |
| timestamp | Event timestamp |
| price | Executed trade price |
| quantity | Cryptocurrency quantity traded |
| trade_id | Unique sample trade identifier |
| source | Source identifier |

---

## Kline Event Schema

Kline events represent OHLCV candlestick market data.

Example:

```json
{
  "event_type": "kline",
  "symbol": "BTCUSDT",
  "timestamp": "2026-08-31T10:00:00.000Z",
  "interval": "1m",
  "open": 112440.00,
  "high": 112485.50,
  "low": 112428.20,
  "close": 112471.35,
  "volume": 12.845,
  "source": "binance_sample"
}
```

### Kline Fields

| Field | Description |
|---|---|
| event_type | Type of streaming event |
| symbol | Cryptocurrency trading pair |
| timestamp | Candlestick timestamp |
| interval | Kline interval |
| open | Opening price |
| high | Highest price |
| low | Lowest price |
| close | Closing price |
| volume | Trading volume |
| source | Source identifier |

The kline records maintain logical OHLC relationships, and consecutive candles use continuous prices to make the sample data more realistic.

---

## Kafka Topics

Two dedicated Kafka topics are used.

| Topic | Purpose | Partitions |
|---|---|---:|
| `crypto_trades` | Cryptocurrency trade events | 3 |
| `crypto_klines` | OHLCV/kline events | 3 |

Replication factor used for the local development environment:

```text
1
```

The trading symbol is used as the Kafka message key.

This allows events for the same symbol to be consistently partitioned by Kafka.

---

## Kafka Topic Creation

Trade topic:

```bash
docker exec sda-kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic crypto_trades \
  --partitions 3 \
  --replication-factor 1
```

Kline topic:

```bash
docker exec sda-kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic crypto_klines \
  --partitions 3 \
  --replication-factor 1
```

Topics can be inspected using:

```bash
docker exec sda-kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic crypto_trades
```

and:

```bash
docker exec sda-kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic crypto_klines
```

---

## Producer

`producer.py` performs the ingestion stage.

It:

1. Loads trade and kline sample datasets
2. Connects to the Kafka broker
3. Serializes records as JSON
4. Uses the cryptocurrency symbol as the Kafka key
5. Publishes trade events to `crypto_trades`
6. Publishes kline events to `crypto_klines`
7. Simulates continuous streaming using a configurable delay
8. Waits for Kafka acknowledgements
9. Displays topic, symbol, partition and offset information
10. Handles errors and graceful keyboard interruption

Example producer output:

```text
✓ Sent | Topic: crypto_trades | Symbol: BTCUSDT | Price: 112,450.20 | Qty: 0.0042 | Partition: 2 | Offset: 24

✓ Sent | Topic: crypto_klines | Symbol: BTCUSDT | Close: 112,471.35 | Volume: 12.845 | Partition: 2 | Offset: 24
```

---

## Consumer

`consumer.py` subscribes to both Kafka topics:

```text
crypto_trades
crypto_klines
```

It deserializes JSON events and displays both event data and Kafka metadata.

Example:

```text
✓ Received | Topic: crypto_trades | Partition: 2 | Offset: 24 | Key: BTCUSDT

TRADE | Symbol: BTCUSDT | Price: 112450.2 | Quantity: 0.0042 | Trade ID: 100001
```

For kline events:

```text
✓ Received | Topic: crypto_klines | Partition: 2 | Offset: 24 | Key: BTCUSDT

KLINE | Symbol: BTCUSDT | Interval: 1m | O: 112440.0 | H: 112485.5 | L: 112428.2 | C: 112471.35 | Volume: 12.845
```

---

## Installation

Python 3 is required.

Install the project dependency with:

```bash
pip install -r requirements.txt
```

The Python dependency used by this implementation is:

```text
kafka-python==3.0.9
```

A running Apache Kafka broker is also required.

The local development configuration used for this project exposes Kafka at:

```text
localhost:9092
```

---

## Running the Project

Start Kafka before running the Python applications.

### Terminal 1 — Consumer

```bash
python3 consumer.py
```

The consumer waits for incoming cryptocurrency events.

### Terminal 2 — Producer

```bash
python3 producer.py
```

The producer continuously publishes sample trade and kline records.

Stop either application using:

```text
Ctrl + C
```

Both programs include graceful shutdown handling.

---

## Kafka Message Flow Demonstration

The following screenshot demonstrates the complete Kafka message flow.

- Left terminal: Kafka consumer receiving events
- Right terminal: Kafka producer publishing events
- Trade and kline topics are both visible
- Multiple cryptocurrency symbols are visible
- Kafka partitions are visible
- Kafka offsets are visible
- Kafka message keys are visible
- Structured JSON payloads are visible

![Kafka Producer and Consumer Demonstration](screenshots/kafka_stream_demo.png)

---

## Production-Style Design Features

This assignment goes beyond a basic CSV-to-Kafka implementation by including:

- Separate trade and kline Kafka topics
- Structured JSON event schemas
- Multiple cryptocurrency trading pairs
- Realistic sequential market-price movements
- OHLCV candlestick data
- Symbol-based Kafka message keys
- Multiple Kafka partitions
- Kafka acknowledgement handling
- Partition and offset inspection
- Continuous streaming simulation
- Input validation
- Error handling
- Graceful shutdown
- Separate producer and consumer applications

---

## Future Extension

The current architecture is designed to continue naturally into later streaming analytics assignments.

```text
Binance WebSocket / API
          ↓
        Kafka
          ↓
   Python Consumers
          ↓
   Stream Processing
          ↓
     MongoDB / SQL
          ↓
       Analytics
          ↓
       Dashboard
```

The sample-data layer can later be replaced by live Binance WebSocket events without requiring a complete redesign of the downstream Kafka architecture.

---

## Assignment Deliverables

- [x] Realistic sample cryptocurrency trade data
- [x] Realistic sample cryptocurrency kline data
- [x] Kafka producer
- [x] Kafka consumer for demonstration
- [x] `crypto_trades` Kafka topic
- [x] `crypto_klines` Kafka topic
- [x] JSON serialization
- [x] Symbol-based Kafka keys
- [x] Continuous message streaming
- [x] Kafka partition and offset information
- [x] Terminal producer/consumer demonstration
- [x] Screenshot evidence
- [x] Reproducible dependency file

---

## Technologies

- Python
- Apache Kafka
- Docker
- JSON
- kafka-python
- Binance-style cryptocurrency market data

---

## Author

**Mahaveer**

Streaming Data Analytics — Assignment 2  
Sample Data & Kafka Producer
