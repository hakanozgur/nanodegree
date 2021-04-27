"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

# BOOTSTRAP_SERVERS = "PLAINTEXT://kafka0:9092"
# SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
BOOTSTRAP_SERVERS = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # TODO: compression type, batch, timeout ..?
        self.broker_properties = {
            "schema.registry.url": SCHEMA_REGISTRY_URL,
            "bootstrap.servers": BOOTSTRAP_SERVERS
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
            logger.debug(f"Creating topic in producer -> {self.topic_name}")

        self.producer = AvroProducer(
            self.broker_properties,
            default_value_schema=self.value_schema,
            default_key_schema=self.key_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})

        # Return if topic already exists
        topic_list = client.list_topics().topics
        if self.topic_name in topic_list:
            logger.info(f"Topic already exists in producer -> {self.topic_name}")
            return

        # Create the topic
        new_topic = NewTopic(self.topic_name, self.num_partitions, self.num_replicas)
        topic_futures = client.create_topics([new_topic])

        # Check create results
        for topic, future in topic_futures.items():
            try:
                future.result()
                logger.info(f"{topic} -> is created successfully")
            except Exception as e:
                logger.error(f"{topic} -> creation exception -> {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info(f"{self.topic_name} -> is closed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
