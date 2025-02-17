import os
import datetime
import requests
from enum import Enum

BASE_URL = "http://homeassistant.local:8123"


headers = {
    "Authorization": f"Bearer {os.getenv("TOKEN")}",
    "Content-Type": "application/json",
}

SensorKind = Enum("SensorKind", [("TEMPERATURE", 1), ("HUMIDITY", 2), ("BATTERY", 3)])
Period = Enum("Period", [("TODAY", 1), ("YESTERDAY", 2), ("PASTWEEK", 3)])


def get_params(entity_id, period):
    start = end = datetime.datetime.now()
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end.replace(hour=23, minute=59, second=59, microsecond=999999)

    if period == Period.YESTERDAY:
        start = start - datetime.timedelta(days=1)
        end = end - datetime.timedelta(days=1)

    if period == Period.PASTWEEK:
        start = start - datetime.timedelta(weeks=1)
        end = end - datetime.timedelta(weeks=1)

    # # Parameters for the query
    return {
        "filter_entity_id": entity_id,
        "start_time": f"{start.isoformat()}",
        "end_time": f"{end.isoformat()}",
    }


def post_request(url, payload):
    response = requests.post(url, payload)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"status: {response.status_code}")


def get_request(url, params):
    if params:
        response = requests.get(url, headers=headers, params=params)
    else:
        response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"status: {response.status_code}")
        return None


class Measurement:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.__dict__}"


class Sensor:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    @classmethod
    def from_dict(cls, **args):
        return cls(**args)

    def has_attr(self, name, value=None):
        if name in self.attributes:
            return not value or self.attributes[name] == value

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.__dict__}"

    def history(self, period: Period) -> list[Measurement]:
        params = get_params(self.entity_id, period)
        print(params)
        path = "/api/history/period"
        response = get_request(BASE_URL + path, params)
        self._history = [Measurement(**m) for m in response[0]]
        return self._history

    @property
    def type(self):
        return self.entity_id.split(".")[0]

    @property
    def kind(self):
        if self.has_attr("device_class", "temperature"):
            return SensorKind.TEMPERATURE
        if self.has_attr("device_class", "humidity"):
            return SensorKind.HUMIDITY


class HomeAssistant:

    def __init__(self):
        self._states = get_request(BASE_URL + "/api/states", None)
        self._devices = [Sensor.from_dict(**state) for state in self._states]

    def select(self, type: str, kind: SensorKind, id=None):
        if id is None:
            return self.find(lambda d: d.type == type and d.kind == kind)
        else:
            return self.find(
                lambda d: d.type == type and d.kind == kind and d.entity_id == id
            )

    def find(self, filter):
        return [device for device in self._devices if filter(device)]

    @property
    def types(self):
        return list({item.entity_id.split(".")[0] for item in self.devices})

    @property
    def states(self):
        return self._states

    @property
    def devices(self):
        return self._devices
