from homeassistant import HomeAssistant, Sensor, Period
import hashlib
import pandas as pd

ha = HomeAssistant()


def calc_md5(args):
    text = "".join(args)
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def resample_df(lst, id):
    df = pd.DataFrame(lst)

    # Convert timestamp to datetime object
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # Convert temperature to number
    df["temperature"] = pd.to_numeric(df["temperature"])

    # Set timestamp as index
    df.set_index("timestamp", inplace=True)

    # Resample to 30-minute intervals and calculate mean
    df_resampled = df.resample("30min").mean()

    # round to single decimal
    df_resampled["temperature"] = df_resampled["temperature"].round(1)

    # fill missing data with previous value
    df_resampled.fillna(method="ffill")

    # add md5 hash and id columns
    df_resampled[["md5", "id"]] = df_resampled.apply(
        lambda row: pd.Series(
            [calc_md5([id, str(row.name), str(row.temperature)]), id]
        ),
        axis=1,
    )

    return df_resampled


# iterate over all temperature sensors
for sensor in ha.select(type=Sensor.TEMPERATURE):

    lst = []
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

    df = resample_df(lst, sensor.entity_id)
    df.to_csv(f"{sensor.entity_id}.csv")
