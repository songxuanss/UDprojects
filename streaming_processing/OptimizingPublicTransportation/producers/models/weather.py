"""Methods pertaining to weather data"""
from dataclasses import dataclass, asdict
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

from confluent_kafka.avro import loads
import requests

from producers.models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    NUM_PARTITIONS = 3
    NUM_REPLICAS = 1

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    @dataclass
    class WeatherData:
        temperature: int
        status: str


    def __init__(self, month):
        super().__init__(
            topic_name="weather_event",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=self.NUM_PARTITIONS,
            num_replicas=self.NUM_REPLICAS
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
        logger.info("weather kafka proxy integration started")
        logger.info(f"temp: {float(self.temp)}, status: {self.status.name}")
        weather = self.WeatherData(temperature=self.temp, status=self.status.name)

        value_schema = """
        {
          "namespace": "com.udacity",
          "type": "record",
          "name": "weather.value",
          "fields": [
            {"name": "temperature", "type": "float"},
            {"name": "status", "type": "string"}
          ]
        }
        """

        data = {
                   "value_schema": value_schema,
                   "records": [
                       {
                           "value": asdict(weather)
                       }
                   ]
               }
        resp = requests.post(
           f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
           headers=headers,
           data=json.dumps(data)
        )
        try:
            resp.raise_for_status()
        except Exception as e:
            logger.error(json.dumps(resp.json()))
            raise e

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )

if __name__ == '__main__':
    a = Weather(1)
    a.run(1)