import json
from kafka import KafkaConsumer


# ============================================================
# ASSIGNMENT 2 - CRYPTOCURRENCY KAFKA CONSUMER
# ============================================================

BOOTSTRAP_SERVERS = ["localhost:9092"]

TOPICS = [
    "crypto_trades",
    "crypto_klines"
]

CONSUMER_GROUP = "assignment2-demo-consumer"


def create_consumer():
    """
    Create a Kafka consumer subscribed to both Assignment 2 topics.
    """

    consumer = KafkaConsumer(
        *TOPICS,

        bootstrap_servers=BOOTSTRAP_SERVERS,

        group_id=CONSUMER_GROUP,

        auto_offset_reset="latest",

        enable_auto_commit=True,

        key_deserializer=lambda key: (
            key.decode("utf-8")
            if key is not None
            else None
        ),

        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        )
    )

    return consumer


def display_trade(message, event):
    """
    Display a trade event in a readable format.
    """

    print(
        f"TRADE | "
        f"Symbol: {event.get('symbol')} | "
        f"Price: {event.get('price')} | "
        f"Quantity: {event.get('quantity')} | "
        f"Trade ID: {event.get('trade_id')}"
    )


def display_kline(message, event):
    """
    Display a kline/OHLCV event in a readable format.
    """

    print(
        f"KLINE | "
        f"Symbol: {event.get('symbol')} | "
        f"Interval: {event.get('interval')} | "
        f"O: {event.get('open')} | "
        f"H: {event.get('high')} | "
        f"L: {event.get('low')} | "
        f"C: {event.get('close')} | "
        f"Volume: {event.get('volume')}"
    )


def main():

    print("=" * 95)
    print("ASSIGNMENT 2 - CRYPTOCURRENCY KAFKA CONSUMER")
    print("=" * 95)

    print("\nKafka Configuration")
    print("-" * 95)

    print(f"Broker         : {BOOTSTRAP_SERVERS[0]}")
    print(f"Topics         : {', '.join(TOPICS)}")
    print(f"Consumer Group : {CONSUMER_GROUP}")

    print("\nConnecting to Kafka...")

    consumer = None

    try:

        consumer = create_consumer()

        print("✓ Kafka consumer created successfully.")

        print("\nWaiting for cryptocurrency events...")
        print("Press Ctrl+C to stop.\n")

        for message in consumer:

            try:

                event = message.value

                event_type = event.get(
                    "event_type",
                    "unknown"
                )

                print("-" * 95)

                print(
                    f"✓ Received | "
                    f"Topic: {message.topic} | "
                    f"Partition: {message.partition} | "
                    f"Offset: {message.offset} | "
                    f"Key: {message.key}"
                )

                if event_type == "trade":

                    display_trade(
                        message,
                        event
                    )

                elif event_type == "kline":

                    display_kline(
                        message,
                        event
                    )

                else:

                    print(
                        f"UNKNOWN EVENT TYPE: "
                        f"{event_type}"
                    )

                print(
                    "JSON:",
                    json.dumps(
                        event,
                        separators=(",", ":")
                    )
                )

            except (
                json.JSONDecodeError,
                KeyError,
                TypeError,
                ValueError
            ) as error:

                print(
                    f"❌ Invalid message received: "
                    f"{error}"
                )

    except KeyboardInterrupt:

        print(
            "\n\nKeyboard interruption received."
        )

        print(
            "Stopping consumer gracefully..."
        )

    except Exception as error:

        print(
            f"\n❌ Kafka consumer error: "
            f"{error}"
        )

        print(
            "Check that Docker and the Kafka "
            "broker are running on localhost:9092."
        )

    finally:

        if consumer is not None:

            consumer.close()

            print(
                "✓ Kafka consumer closed."
            )

        print(
            "✓ Assignment 2 consumer stopped."
        )


if __name__ == "__main__":
    main()