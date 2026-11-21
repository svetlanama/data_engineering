from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import MY_NAME, TOPICS, kafka_config

admin_client = KafkaAdminClient(**kafka_config)

for name in TOPICS:
    try:
        admin_client.create_topics(
            [NewTopic(name=name, num_partitions=2, replication_factor=1)]
        )
        print(f"Топік створено: {name}")
    except TopicAlreadyExistsError:
        print(f"Топік вже існує: {name}")

print("\nТопіки в Kafka:")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
