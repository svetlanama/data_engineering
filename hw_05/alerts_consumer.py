from configs import HUMIDITY_ALERTS, TEMPERATURE_ALERTS, create_consumer

consumer = create_consumer(
    TEMPERATURE_ALERTS, HUMIDITY_ALERTS, group_id="alerts_reader"
)

print(f"Сповіщення з {TEMPERATURE_ALERTS} та {HUMIDITY_ALERTS}")

try:
    for record in consumer:
        alert = record.value
        print(
            f"[{alert['timestamp']}] {record.topic} | датчик {alert['sensor_id']} | "
            f"{alert['temperature']:.1f} °C, {alert['humidity']:.1f} % | "
            f"{alert['message']}"
        )
except KeyboardInterrupt:
    print("Читання сповіщень зупинено")
finally:
    consumer.close()
