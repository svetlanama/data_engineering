import json

from configs import ALERTS, create_consumer

consumer = create_consumer(ALERTS, group_id="alerts_reader")

print(f"Алерти з {ALERTS}")

try:
    for record in consumer:
        print(json.dumps(record.value, ensure_ascii=False))
except KeyboardInterrupt:
    print("Читання алертів зупинено")
finally:
    consumer.close()
