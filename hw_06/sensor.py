import random
import time
from datetime import datetime, timezone

from configs import BUILDING_SENSORS, create_producer

SEND_INTERVAL_SEC = 2

# Кожен датчик стоїть у своєму приміщенні: має власний базовий рівень
# температури й вологості, навколо якого коливаються показники.
sensor_id = random.randint(1000, 9999)
base_temperature = random.uniform(20, 50)
base_humidity = random.uniform(25, 75)

producer = create_producer()

print(
    f"Датчик {sensor_id} -> {BUILDING_SENSORS} "
    f"(база: {base_temperature:.0f} °C, {base_humidity:.0f} %)"
)

try:
    while True:
        reading = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "temperature": round(base_temperature + random.uniform(-3, 3), 1),
            "humidity": round(base_humidity + random.uniform(-5, 5), 1),
        }
        producer.send(BUILDING_SENSORS, key=sensor_id, value=reading)
        producer.flush()
        print(
            f"[{reading['timestamp']}] датчик {sensor_id}: "
            f"{reading['temperature']:.1f} °C, {reading['humidity']:.1f} %"
        )
        time.sleep(SEND_INTERVAL_SEC)
except KeyboardInterrupt:
    print(f"Датчик {sensor_id} зупинено")
finally:
    producer.close()
