import logging
import os

# Ширина поля имени логера — сообщения выровняются в колонку
LOG_NAME_WIDTH = 38

# Общий логер для приложения
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [%(levelname)s] %(name)-{LOG_NAME_WIDTH}s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Конфигурация для Kafka (в Docker задаётся через KAFKA_BOOTSTRAP_SERVERS)
_KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9094",
)
KAFKA_BOOTSTRAP_CONFIG = {
    "bootstrap.servers": _KAFKA_BOOTSTRAP_SERVERS,
}

SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL",
    "http://localhost:8082",
)

CONSUMER_GROUP_ID_1 = "consumer-group-1"
CONSUMER_GROUP_ID_2 = "consumer-group-2"

BATCH_SIZE = 10

# Названия топика для тестов
TEST_TOPIC_NAME = "test-topic-homework-1"
