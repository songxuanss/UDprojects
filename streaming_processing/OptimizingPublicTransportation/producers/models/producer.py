"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
from common import common

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    BROKER = "broker"
    REGISTRY = "registry"
    BROKER_URL = common.BROKER_URL
    SCHEMA_REGISTRY_URL = common.SCHEMA_REGISTRY_URL

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
        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            self.BROKER: self.BROKER_URL,
            self.REGISTRY: schema_registry
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer_inst: AvroProducer = AvroProducer({"bootstrap.servers": self.broker_properties[self.BROKER]},
                                                        schema_registry=self.broker_properties[self.REGISTRY])

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # TODO: Write code that creates the topic for this producer if it does not already exist on
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
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        if self.producer_inst:
            self.producer_inst.flush()
            self.producer_inst = None

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))


if __name__ == '__main__':
    pass