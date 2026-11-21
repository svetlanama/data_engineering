import json
import os

from kafka import KafkaConsumer, KafkaProducer

MY_NAME = "svitlana_moiseyenko"

BUILDING_SENSORS = f"{MY_NAME}_building_sensors"
ALERTS = f"{MY_NAME}_alerts"
TOPICS = [BUILDING_SENSORS, ALERTS]

kafka_config = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
}

if kafka_config["security_protocol"].startswith("SASL"):
    kafka_config.update(
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    )


def create_producer():
    """Створює продюсера, що серіалізує ключі та значення у JSON."""
    return KafkaProducer(
        **kafka_config,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def create_consumer(*topics, group_id):
    """Створює консьюмера, підписаного на вказані топіки."""
    return KafkaConsumer(
        *topics,
        **kafka_config,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
