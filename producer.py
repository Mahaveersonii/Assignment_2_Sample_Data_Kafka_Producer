import json
import time
from pathlib import Path

from kafka import KafkaProducer


# ============================================================
# ASSIGNMENT 2 - CRYPTOCURRENCY KAFKA PRODUCER
# ============================================================

BOOTSTRAP_SERVERS = ["localhost:9092"]

TRADE_TOPIC = "crypto_trades"
KLINE_TOPIC = "crypto_klines"

# Delay between messages to simulate real-time streaming
STREAM_DELAY = 0.7

BASE_DIR = Path(__file__).resolve().parent

TRADE_FILE = BASE_DIR / "data" / "sample_trades.json"
KLINE_FILE = BASE_DIR / "data" / "sample_klines.json"


def load_json_file(file_path):
    """
    Load and validate a JSON sample dataset.
    """

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            records = json.load(file)

        if not isinstance(records, list):
            raise ValueError(
                f"{file_path.name} must contain a JSON array."
            )

        if len(records) == 0:
            raise ValueError(
                f"{file_path.name} contains no records."
            )

        return records

    except FileNotFoundError:
        print(
            f"❌ Sample data file not found: {file_path}"
        )
        raise

    except json.JSONDecodeError as error:
        print(
            f"❌ Invalid JSON in {file_path.name}: {error}"
        )
        raise


def validate_record(record):
    """
    Validate common fields required by all events.
    """

    required_fields = [
        "event_type",
        "symbol",
        "timestamp",
        "source"
    ]

    missing_fields = [
        field
        for field in required_fields
        if field not in record
    ]

    if missing_fields:
        raise ValueError(
            "Missing required field(s): "
            + ", ".join(missing_fields)
        )


def create_producer():
    """
    Create Kafka producer using JSON serialization.
    """

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,

        key_serializer=lambda key: (
            key.encode("utf-8")
        ),

        value_serializer=lambda value: (
            json.dumps(value).encode("utf-8")
        ),

        acks="all",

        retries=3
    )

    return producer


def send_record(producer, topic, record):
    """
    Send one event to Kafka and return delivery metadata.
    """

    validate_record(record)

    symbol = record["symbol"]

    future = producer.send(
        topic,
        key=symbol,
        value=record
    )

    # Wait for Kafka acknowledgement
    metadata = future.get(timeout=10)

    event_type = record["event_type"]

    if event_type == "trade":

        details = (
            f"Price: {record['price']:,.2f} | "
            f"Qty: {record['quantity']}"
        )

    elif event_type == "kline":

        details = (
            f"Close: {record['close']:,.2f} | "
            f"Volume: {record['volume']}"
        )

    else:

        details = (
            f"Event Type: {event_type}"
        )

    print(
        f"✓ Sent | "
        f"Topic: {metadata.topic} | "
        f"Symbol: {symbol} | "
        f"{details} | "
        f"Partition: {metadata.partition} | "
        f"Offset: {metadata.offset}"
    )


def main():

    print("=" * 95)
    print("ASSIGNMENT 2 - CRYPTOCURRENCY KAFKA PRODUCER")
    print("=" * 95)

    print("\nLoading Binance-style sample datasets...")

    try:

        trades = load_json_file(
            TRADE_FILE
        )

        klines = load_json_file(
            KLINE_FILE
        )

    except (
        FileNotFoundError,
        json.JSONDecodeError,
        ValueError
    ):
        return

    print(
        f"✓ Trade events loaded : {len(trades)}"
    )

    print(
        f"✓ Kline events loaded : {len(klines)}"
    )

    print("\nKafka Configuration")
    print("-" * 95)

    print(
        f"Broker      : {BOOTSTRAP_SERVERS[0]}"
    )

    print(
        f"Trade Topic : {TRADE_TOPIC}"
    )

    print(
        f"Kline Topic : {KLINE_TOPIC}"
    )

    print(
        f"Message Key : Trading symbol"
    )

    print("\nConnecting to Kafka...")

    producer = None

    try:

        producer = create_producer()

        # Force Kafka metadata lookup so connection
        # problems appear before streaming begins.
        producer.partitions_for(
            TRADE_TOPIC
        )

        producer.partitions_for(
            KLINE_TOPIC
        )

        print(
            "✓ Kafka producer connected successfully."
        )

        print(
            "\nStreaming started."
        )

        print(
            "Press Ctrl+C to stop.\n"
        )

        trade_index = 0
        kline_index = 0

        while True:

            # ----------------------------------------
            # TRADE EVENT
            # ----------------------------------------

            trade = trades[trade_index]

            try:

                send_record(
                    producer,
                    TRADE_TOPIC,
                    trade
                )

            except (
                KeyError,
                TypeError,
                ValueError
            ) as error:

                print(
                    f"❌ Invalid trade event: {error}"
                )

            except Exception as error:

                print(
                    f"❌ Kafka trade send error: {error}"
                )

            trade_index = (
                trade_index + 1
            ) % len(trades)

            time.sleep(
                STREAM_DELAY
            )

            # ----------------------------------------
            # KLINE EVENT
            # ----------------------------------------

            kline = klines[kline_index]

            try:

                send_record(
                    producer,
                    KLINE_TOPIC,
                    kline
                )

            except (
                KeyError,
                TypeError,
                ValueError
            ) as error:

                print(
                    f"❌ Invalid kline event: {error}"
                )

            except Exception as error:

                print(
                    f"❌ Kafka kline send error: {error}"
                )

            kline_index = (
                kline_index + 1
            ) % len(klines)

            time.sleep(
                STREAM_DELAY
            )

    except KeyboardInterrupt:

        print(
            "\n\nKeyboard interruption received."
        )

        print(
            "Stopping producer gracefully..."
        )

    except Exception as error:

        print(
            f"\n❌ Kafka producer error: {error}"
        )

        print(
            "Check that Docker and the Kafka "
            "broker are running on localhost:9092."
        )

    finally:

        if producer is not None:

            try:
                producer.flush()
                producer.close()

                print(
                    "✓ Kafka producer closed."
                )

            except Exception as error:

                print(
                    f"⚠ Error while closing producer: {error}"
                )

        print(
            "✓ Assignment 2 producer stopped."
        )


if __name__ == "__main__":
    main()