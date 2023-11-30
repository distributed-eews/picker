import os
from app.container import KafkaContainer
from dotenv import load_dotenv
import time

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
TOPIC_CONSUMER = os.getenv('TOPIC_CONSUMER')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
MONGO_DB = os.getenv('MONGO_DB', 'eews')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'parameters')
PROMETHEUS_ADDR = os.getenv('PROMETHEUS_ADDR', '0.0.0.0')
PROMETHEUS_PORT = os.getenv('PROMETHEUS_PORT', '8012')

if __name__ == "__main__":
    container = KafkaContainer()
    container.config.from_dict(
        {
            'bootstrap_servers': BOOTSTRAP_SERVERS,
            'kafka_config': {
                'bootstrap.servers': BOOTSTRAP_SERVERS,
                'group.id': 'picker',
                'auto.offset.reset': 'earliest',
            },
            'redis': {
                'host': REDIS_HOST,
                'port': int(REDIS_PORT),
            },
            'mongo': {
                'db_name': MONGO_DB,
                'host': MONGO_HOST,
                'port': int(MONGO_PORT),
                'collection': MONGO_COLLECTION,
            },
            'prometheus': {
                'addr': PROMETHEUS_ADDR,
                'port': int(PROMETHEUS_PORT),
            }
        }, True)
    promethues = container.prometheus()
    promethues.start()
    data_processor = container.data_processor()
    print("=" * 20 + f"Consuming Data From {TOPIC_CONSUMER} Topic" + "=" * 20)
    data_processor.consume(TOPIC_CONSUMER)
