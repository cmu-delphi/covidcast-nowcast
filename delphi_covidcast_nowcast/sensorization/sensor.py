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
                    lag: int,
                    sensors: List[SignalConfig],
                    ground_truth_sensor: SignalConfig,
                    ground_truths: List[LocationSeries],
                    export_data: bool
                    ) -> DefaultDict[SignalConfig, List[LocationSeries]]:
    """

    Parameters
    ----------
    as_of_date
        Date that the data should be retrieved as of.
    lag
        Number of days before as_of_date to generate the sensor for.
    sensors
        list of SignalConfigs for sensors to compute
    ground_truths
        list of LocationSeries, one for each location desired.
    ground_truth_sensor
        SignalConfig of the ground truth
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
    predicted_date = int((datetime.strptime(str(as_of_date), "%Y%m%d") -
                          timedelta(lag)).strftime("%Y%m%d"))
    indicator_data = get_indicator_data(sensors, ground_truths, predicted_date, as_of_date)
    for loc in ground_truths:
        ar_sensor = compute_ar_sensor(predicted_date, loc)
        if not np.isnan(ar_sensor):
            output[ground_truth_sensor].append(
                LocationSeries(loc.geo_value, loc.geo_type, [predicted_date], [ar_sensor])
            )
        for sensor in sensors:
            covariates = indicator_data.get(
                (sensor.source, sensor.signal, loc.geo_type, loc.geo_value)
            )
            if not covariates:
                print(f"No data: {(sensor.source, sensor.signal, loc.geo_type, loc.geo_value)}")
                continue
            reg_sensor = compute_regression_sensor(predicted_date, covariates, loc)
            if not np.isnan(reg_sensor):
                output[sensor].append(
                    LocationSeries(loc.geo_value, loc.geo_type, [predicted_date], [reg_sensor])
                )
    if export_data:
        pass  # TODO fill this in
    return output


def historical_sensors(start_date: int,
                       end_date: int,
                       lag: int,
                       sensors: List[SignalConfig],
                       ground_truth_sensor: SignalConfig,
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
        list of SignalConfigs for sensors to retrieve
    ground_truths
        list of LocationSeries, one for each location desired.
    ground_truth_sensor
        SignalConfig of the ground truth

    Returns
    -------
        Dict where keys are sensor tuples and values are lists, where each list element is a
        LocationSeries holding sensor data for a location.
    """
    output = defaultdict(list)
    for location in ground_truths:
        ar_sensor, missing_dates = get_historical_sensor_data(
            ground_truth_sensor, location.geo_type, location.geo_value, start_date, end_date, lag
        )
        if not ar_sensor.empty:
            output[ground_truth_sensor].append(ar_sensor)
        for sensor in sensors:
            reg_sensor, missing_dates = get_historical_sensor_data(
                sensor, location.geo_type, location.geo_value, start_date, end_date, lag
            )
            if not reg_sensor.empty:
                output[sensor].append(reg_sensor)
    return output


def _export_to_csv(value,
                   sensor,
                   geo_type,
                   geo_value,
                   receiving_dir="/common/covidcast_nowcast/receiving"  # convert this to use params file
                   ) -> str:
    """Save value to csv for upload to epidata database

    NOT DONE YET
    """
    export_dir = os.path.join(receiving_dir, sensor.source)
    os.makedirs(export_dir, exist_ok=True)
    export_file = os.path.join(export_dir, f"{sensor.lagged_date}_{geo_type}_{sensor.signal}.csv")
    with open(export_file, "w") as f:
        f.write("sensor_name,geo_value,value,lag\n")
        f.write(f"{sensor.name},{geo_value},{value},{sensor.lag}\n")
    return export_file

