"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    NUM_PARTITIONS = 3
    NUM_REPLICAS = 1
    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    print (f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")
    print (value_schema)
    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            "turnstile_event",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=self.NUM_PARTITIONS,
            num_replicas=self.NUM_REPLICAS
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)


    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info("turnstile kafka integration incomplete - skipping")
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #

        logger.debug(self.value_schema)
        self.producer.produce(
            topic=self.topic_name,
            key={"timestamp": self.time_millis()},
            value={
                "station_id": self.station.station_id,
                "line": self.station.color.name,
                "station_name": self.station.name
            },
            value_schema=self.value_schema,
            key_schema=self.key_schema
        )