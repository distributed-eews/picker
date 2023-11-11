import os
from app.container import KafkaContainer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
TOPIC_CONSUMER = os.getenv('TOPIC_CONSUMER')


if __name__ == "__main__":
    container = KafkaContainer()
    container.config.from_dict(
        {
            'bootstrap_servers': BOOTSTRAP_SERVERS,
            'kafka_config': {
                'bootstrap.servers': BOOTSTRAP_SERVERS,
                'group.id': 'picker',
                'auto.offset.reset': 'earliest',
            }
        }, True)
    data_processor = container.data_processor()
    print("=" * 20 + f"Consuming Data From {TOPIC_CONSUMER} Topic" + "=" * 20)
    data_processor.consume(TOPIC_CONSUMER)
