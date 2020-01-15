"""Defines trends calculations for stations"""
import logging

import faust
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record, serializer="json"):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record, serializer="json"):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream2", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.cta.stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "station_summary3",
    default=TransformedStation,
    changelog_topic=out_topic,
    partitions=1
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def station_event(station_events):
    async for se in station_events.group_by(Station.station_name):
        line=""
        if se.red == True:
            line = "red"
        elif se.blue == True:
            line = "blue"
        elif se.green == True:
            line = "green"

        transformedStation = TransformedStation(station_id=se.station_id,
                                                station_name=se.station_name, order=se.order, line=line)

        table[se.station_name] = asdict(transformedStation)
        print("finish transformation")

if __name__ == "__main__":
    app.main()
