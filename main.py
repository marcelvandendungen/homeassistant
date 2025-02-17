from homeassistant import HomeAssistant, SensorKind, Period
import hashlib
import pandas as pd
from pathlib import Path

ha = HomeAssistant()


def create_directories(sensors):
    for sensor in sensors:
        p = Path(sensor.entity_id.split(".")[1])
        p.mkdir(exist_ok=True)


def calc_md5(args):
    text = "".join(args)
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def resample_df(df):

    # Convert timestamp to datetime object and local timezone
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("US/Pacific")

    # df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
    # Convert temperature to number
    df["temperature"] = pd.to_numeric(df["temperature"])

    # Set timestamp as index
    # df.set_index("timestamp", inplace=True)

    # Resample to 30-minute intervals and calculate mean
    df_resampled = df.resample("30min", on="timestamp").mean()

    # round to single decimal
    df_resampled["temperature"] = df_resampled["temperature"].round(1)

    # fill missing data with previous value
    df_resampled["temperature"] = df_resampled["temperature"].ffill()

    # add md5 hash and id columns
    df_resampled[["md5"]] = df_resampled.apply(
        lambda row: pd.Series([calc_md5([str(row.name), str(row.temperature)])]),
        axis=1,
    )

    df = df_resampled[["md5", "temperature"]]
    return df


def get_sensor_data(kind: SensorKind):
    lst = []

    # iterate over all temperature sensors
    for sensor in ha.select(type="sensor", kind=kind):

        # for each sensor, iterate over all measurements
        for measurement in sensor.history(Period.TODAY):
            # skip unavailable measurements
            if measurement.state != "unavailable":
                lst.append(
                    {
                        "timestamp": measurement.last_updated,
                        "temperature": measurement.state,
                    }
                )

        yield sensor.entity_id.split(".")[1], lst


def write_csvs(lst, raw_name, filepath):
    if lst:
        df = pd.DataFrame(lst)
        df.to_csv(raw_name)

        df = resample_df(df)

        cur_df = (
            pd.read_csv(filepath, index_col="timestamp")
            if Path(filepath).exists()
            else pd.DataFrame()
        )

        df = pd.concat([cur_df, df])  # ignore_index=True removes datetime column
        df = df.drop_duplicates(subset="md5")
        df.to_csv(filepath)


def main():
    # create directory per sensor
    temp_sensors = ha.select(type="sensor", kind=SensorKind.TEMPERATURE)
    hum_sensors = ha.select(type="sensor", kind=SensorKind.HUMIDITY)
    create_directories(temp_sensors)
    create_directories(hum_sensors)

    for id, lst in get_sensor_data(SensorKind.TEMPERATURE):
        write_csvs(lst, f"{id}/raw_tmp.csv", f"{id}/temperature.csv")

    for id, lst in get_sensor_data(SensorKind.HUMIDITY):
        write_csvs(lst, f"{id}_raw_hum.csv", f"{id}/humidity.csv")


if __name__ == "__main__":
    main()
