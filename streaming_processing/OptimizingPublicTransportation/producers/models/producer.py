"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
import common
import topic_check

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    BROKER_URL = common.BROKER_URL
    SCHEMA_REGISTRY_URL = common.SCHEMA_REGISTRY_URL
    TOPIC_LIST_CACHED = None
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

        schema_registry = CachedSchemaRegistryClient(self.SCHEMA_REGISTRY_URL)

        self.broker_properties = {
            "bootstrap.servers": self.BROKER_URL,
            "linger.ms": 500,
            "acks": 1,
            "retries": 3,
            "message.max.bytes": 4096,
            "batch.num.messages": 10
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer: AvroProducer = AvroProducer(self.broker_properties,
                                                   schema_registry=schema_registry)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        logger.info(f"check topic {self.topic_name}")
        if topic_check.topic_exists(self.topic_name.strip()):
            logger.info(f"topic {self.topic_name} exist")
            return

        # if not Producer.TOPIC_LIST_CACHED or len(Producer.TOPIC_LIST_CACHED) == 0:
        #     Producer.TOPIC_LIST_CACHED = topic_check.list_topics()
        #     logger.info(f"get list of topics cached,,,{Producer.TOPIC_LIST_CACHED}")
        #
        # if self.topic_name in Producer.TOPIC_LIST_CACHED:
        #     logger.info(f"topic {self.topic_name} exist")
        #     return

        client = AdminClient({"bootstrap.servers": self.BROKER_URL})
        futures = client.create_topics([NewTopic(topic=self.topic_name, num_partitions=self.num_partitions,
                                                 replication_factor=self.num_replicas)])
        for _, future in futures.items():
            try:
                future.result()
            except Exception as e:
                logger.error("error when creating topics", e)
                raise e

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer:
            self.producer.flush()
            self.producer = None

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))


if __name__ == '__main__':
    pass