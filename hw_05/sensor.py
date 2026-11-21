import random
import time
from datetime import datetime

from configs import BUILDING_SENSORS, create_producer

SEND_INTERVAL_SEC = 2

sensor_id = random.randint(1000, 9999)
producer = create_producer()

print(f"Датчик {sensor_id} -> {BUILDING_SENSORS}")

try:
    while True:
        reading = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "temperature": round(random.uniform(25, 45), 1),
            "humidity": round(random.uniform(15, 85), 1),
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
