from configs import (
    BUILDING_SENSORS,
    HUMIDITY_ALERTS,
    TEMPERATURE_ALERTS,
    create_consumer,
    create_producer,
)

MAX_TEMPERATURE = 40
MIN_HUMIDITY = 20
MAX_HUMIDITY = 80


def find_alerts(reading):
    """Повертає пари (топік, повідомлення) для показників поза межами норми."""
    alerts = []
    if reading["temperature"] > MAX_TEMPERATURE:
        alerts.append(
            (TEMPERATURE_ALERTS, f"Температура вище {MAX_TEMPERATURE} °C")
        )
    if reading["humidity"] > MAX_HUMIDITY:
        alerts.append((HUMIDITY_ALERTS, f"Вологість вище {MAX_HUMIDITY} %"))
    elif reading["humidity"] < MIN_HUMIDITY:
        alerts.append((HUMIDITY_ALERTS, f"Вологість нижче {MIN_HUMIDITY} %"))
    return alerts


consumer = create_consumer(BUILDING_SENSORS, group_id="sensor_processor")
producer = create_producer()

print(f"Обробка {BUILDING_SENSORS}")

try:
    for record in consumer:
        reading = record.value
        print(
            f"Отримано: датчик {reading['sensor_id']}, "
            f"{reading['temperature']:.1f} °C, {reading['humidity']:.1f} %"
        )
        for topic, message in find_alerts(reading):
            producer.send(
                topic, key=reading["sensor_id"], value={**reading, "message": message}
            )
            print(f"  -> {topic}: {message}")
        producer.flush()
except KeyboardInterrupt:
    print("Обробку зупинено")
finally:
    consumer.close()
    producer.close()
