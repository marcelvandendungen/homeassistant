from homeassistant import HomeAssistant, Sensor, Period
from csv_writer import CsvWriter
import hashlib

ha = HomeAssistant()


def calc_md5(args):
    text = "".join(args)
    return hashlib.md5(text.encode("utf-8")).hexdigest()


with CsvWriter("temperature.csv") as writer:

    # iterate over all temperature sensors
    for sensor in ha.select(type=Sensor.TEMPERATURE):
        # for each sensor, iterate over all measurements
        devices = ha.select(type=Sensor.TEMPERATURE, id=sensor.entity_id)
        print(devices)
        device = devices[0]

        for measurement in device.history(Period.TODAY):
            lst = [measurement.last_updated, measurement.state, measurement.entity_id]
            # write MD5 hash, datetime, temp, id to CSV file
            writer.write(calc_md5(lst), *lst)

with CsvWriter("humidity.csv") as writer:

    for sensor in ha.select(type=Sensor.HUMIDITY):
        devices = ha.select(type=Sensor.HUMIDITY, id=sensor.entity_id)
        print(devices)
        device = devices[0]

        for measurement in device.history(Period.TODAY):
            lst = [measurement.last_updated, measurement.state, measurement.entity_id]
            writer.write(calc_md5(lst), *lst)
