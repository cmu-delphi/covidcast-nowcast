"""Functions to run sensorization."""
import os
from collections import defaultdict
from typing import List, DefaultDict
from datetime import datetime, timedelta

import numpy as np

from .ar_model import compute_ar_sensor
from .get_epidata import get_indicator_data, get_historical_sensor_data
from .regression_model import compute_regression_sensor
from ..data_containers import LocationSeries, SignalConfig


def compute_sensors(as_of_date: int,
                    regression_sensors: List[SignalConfig],
                    ground_truth_sensor: SignalConfig,
                    ground_truths: List[LocationSeries],
                    export_data: bool
                    ) -> DefaultDict[SignalConfig, List[LocationSeries]]:
    """

    Parameters
    ----------
    as_of_date
        Date that the data should be retrieved as of.
    regression_sensors
        list of SignalConfigs for regression sensors to compute.
    ground_truth_sensor
        SignalConfig of the ground truth signal which is used for the AR sensor.
    ground_truths
        list of LocationSeries, one for each location desired.
    export_data
        boolean specifying whether computed regression sensors should be saved out to CSVs.


    Returns
    -------
        Dict where keys are sensor tuples and values are lists, where each list element is a
        LocationSeries holding sensor data for a location. Each LocationSeries will only have a
        single value for the date (as_of_date - lag), e.g. if as_of_date is 20210110 and lag=5,
        the output will be values for 20200105.
    """
    output = defaultdict(list)
    indicator_data = get_indicator_data(regression_sensors, ground_truths, as_of_date)
    for loc in ground_truths:
        ground_truth_pred_date = _lag_date(as_of_date, ground_truth_sensor.lag)
        ar_sensor = compute_ar_sensor(ground_truth_pred_date, loc)
        if not np.isnan(ar_sensor):
            output[ground_truth_sensor].append(
                LocationSeries(loc.geo_value, loc.geo_type, [ground_truth_pred_date], [ar_sensor])
            )
        for sensor in regression_sensors:
            sensor_pred_date = _lag_date(as_of_date, sensor.lag)
            covariates = indicator_data.get(
                (sensor.source, sensor.signal, loc.geo_type, loc.geo_value)
            )
            if not covariates:
                print(f"No data: {(sensor.source, sensor.signal, loc.geo_type, loc.geo_value)}")
                continue
            reg_sensor = compute_regression_sensor(sensor_pred_date, covariates, loc)
            if not np.isnan(reg_sensor):
                output[sensor].append(
                    LocationSeries(loc.geo_value, loc.geo_type, [sensor_pred_date], [reg_sensor])
                )
    if export_data:
        for sensor, locations in output.items():
            for loc in locations:
                print(_export_to_csv(loc, sensor, as_of_date))
    return output


def historical_sensors(start_date: int,
                       end_date: int,
                       sensors: List[SignalConfig],
                       ground_truths: List[LocationSeries],
                       ) -> DefaultDict[SignalConfig, List[LocationSeries]]:
    """
    Retrieve past sensorized values from start to end date at given locations for specified sensors.


    Parameters
    ----------
    start_date
        first day to attempt to get sensor values for.
    end_date
        last day to attempt to get sensor values for.
    lag
        Number of days between a desired sensor date and the data used to compute it. For example,
        a sensor value on 2020-01-01 on lag 5 will use data as_of 2020-01-06.
    sensors
        list of SignalConfigs for sensors to retrieve.
    ground_truths
        list of LocationSeries, one for each location desired.

    Returns
    -------
        Dict where keys are sensor tuples and values are lists, where each list element is a
        LocationSeries holding sensor data for a location.
    """
    output = defaultdict(list)
    for location in ground_truths:
        for sensor in sensors:
            sensor_vals, missing_dates = get_historical_sensor_data(
                sensor, location.geo_value, location.geo_type, start_date, end_date
            )
            if not sensor_vals.empty:
                output[sensor].append(sensor_vals)
    return output


def _export_to_csv(value: LocationSeries,
                   sensor: SignalConfig,
                   as_of_date: int,
                   receiving_dir: str = "./receiving"  # convert this to use params file and eventually be /common/covidcast_nowcast/receiving/
                   ) -> str:
    """Save value to csv for upload to epidata database

    NOT DONE YET
    """
    export_dir = os.path.join(receiving_dir, sensor.source)
    os.makedirs(export_dir, exist_ok=True)
    time_value = value.dates[0]
    export_file = os.path.join(export_dir, f"{time_value}_{value.geo_type}_{sensor.signal}.csv")
    if os.path.exists(export_file):
        with open(export_file, "a") as f:
            f.write(
                f"{sensor.name},{value.geo_value},{value.get_value(time_value)},{as_of_date}\n")
    else:
        with open(export_file, "a") as f:
            f.write("sensor_name,geo_value,value,issue\n")
            f.write(
                f"{sensor.name},{value.geo_value},{value.get_value(time_value)},{as_of_date}\n")

    return export_file


def _lag_date(date, lag):
    return int((datetime.strptime(str(date), "%Y%m%d") - timedelta(lag)).strftime("%Y%m%d"))