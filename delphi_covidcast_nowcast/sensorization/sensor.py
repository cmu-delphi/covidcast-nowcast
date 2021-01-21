"""Functions to run sensorization."""
import os
from typing import List, Tuple, DefaultDict
from collections import defaultdict


import numpy as np
from delphi_epidata import Epidata
# from ..delphi_epidata import Epidata  # used for local testing
from pandas import date_range

from .ar_model import compute_ar_sensor
from .regression_model import compute_regression_sensor
from ..data_containers import LocationSeries, SignalConfig


def get_sensors(start_date: int,
                end_date: int,
                sensors: List[SignalConfig],
                ground_truths: List[LocationSeries],
                compute_missing: bool,
                use_latest_issue: bool,
                export_data: bool,
                ) -> DefaultDict[SignalConfig, List[LocationSeries]]:
    """
    Return sensorized values from start to end date at given locations for specified sensors.

    If compute_missing is True, we attempt to recompute values which cannot be retrieved from
    the Epidata db based on most recent covidcast data.

    Only locations that have complete ground truths (no nans) will have sensorization values
    retrieved or computed, even if compute_missing=False and the ground truth is not needed. This
    behavior should probably be updated.

    Parameters
    ----------
    start_date
        first day to attempt to get sensor values for.
    end_date
        last day to attempt to get sensor values for.
    sensors
        list of tuples specifying (source, signal, sensor_name, model) for sensors
    ground_truths
        list of LocationSeries, one for each location desired. If `compute_missing=False`, ground
        truth is not needed because no training is occuring, and this argument is ignored.
    compute_missing
        boolean specifying whether the function should attempt to compute any dates which
        were not retrieved from historical data. Defaults to False.
    use_latest_issue
        boolean specifying whether to use the latest issue to compute missing sensor values. If
        False, will use the data that was available as of the target date.
    export_data
        boolean specifying whether computed regression sensors should be saved out to CSVs.

    Returns
    -------
        Dict where keys are sensor tuples and values are lists, where each list element is a
        LocationSeries holding sensor data for a location.
    """
    output = defaultdict(list)
    for location_truth in ground_truths:
        ar_sensor = get_ar_sensor_values(location_truth, start_date, end_date)
        if not ar_sensor.empty:
            output["ground_truth_ar"].append(ar_sensor)
        for sensor in sensors:
            reg_sensor = get_regression_sensor_values(sensor,
                                                      start_date,
                                                      end_date,
                                                      location_truth,
                                                      compute_missing,
                                                      use_latest_issue,
                                                      export_data)
            if not reg_sensor.empty:
                output[sensor].append(reg_sensor)
    return output


def get_ar_sensor_values(values: LocationSeries,
                         start_date: int,
                         end_date: int) -> LocationSeries:
    """
    Compute sensorized values for the given date range with an AR model.

    Parameters
    ----------
    values
        LocationSeries of values used to train and predict sensor value.
    start_date
        first day to attempt to get sensor values for.
    end_date
        last day to attempt to get sensor values for.

    Returns
    -------
        LocationSeries of sensor data for the dates requested.
    """
    output = LocationSeries(values.geo_value, values.geo_type, [], [])
    for day in [int(i.strftime("%Y%m%d")) for i in date_range(str(start_date), str(end_date))]:
        sensor_value = compute_ar_sensor(day, values)
        if np.isnan(sensor_value):
            continue
        output.add_data(day, sensor_value)  # if np array would need to change append method
    return output


def get_regression_sensor_values(sensor: SignalConfig,
                                 start_date: int,
                                 end_date: int,
                                 ground_truth: LocationSeries,
                                 compute_missing: bool,
                                 use_latest_issue: bool,
                                 export_data: bool) -> LocationSeries:
    """
    Return sensorized values for a single location, using available historical data if specified.

    If new values are to be computed, they currently are done with the most recent issue of data,
    as opposed to the data available as_of the desired date.
    
    Parameters
    ----------
    sensor
        (source, signal, sensor_name, model) tuple specifying which sensor to retrieve/compute.
    start_date
        first day to attempt to get sensor values for.
    end_date
        last day to attempt to get sensor values for.
    ground_truth
        LocationSeries containing ground truth values to train against. Also used to transfer geo
        information. Values are ignored if compute_missing=False.
    compute_missing
        Flag for whether or not missing values should be recomputed.
    use_latest_issue
        boolean specifying whether to use the latest issue to compute missing sensor values. If
        False, will use the data that was available as of the target date.
    export_data
        boolean specifying whether computed regression sensors should be saved out to CSVs.

    Returns
    -------
        LocationSeries of sensor data.
    """
    # left out recompute_all_data argument for now just to keep things simple
    output, missing_dates = _get_historical_data(
        sensor, ground_truth.geo_type, ground_truth.geo_value,  start_date, end_date
    )
    if (not compute_missing) or (not missing_dates):
        return output
    print(f"Missing dates: {missing_dates}")
    # gets all available data for now, could be optimized to only get a window
    if use_latest_issue:
        response = Epidata.covidcast(data_source=sensor.source,
                                     signals=sensor.signal,
                                     time_type="day",
                                     time_values=Epidata.range(20200101, max(missing_dates)),
                                     geo_value=ground_truth.geo_value,
                                     geo_type=ground_truth.geo_type)
        if response["result"] == -2:
            print("No indicator data available.")
            return output
        elif response["result"] != 1:
            raise Exception(f"Bad result from Epidata: {response['message']}")
        indicator_values = LocationSeries(
            geo_value=ground_truth.geo_value,
            geo_type=ground_truth.geo_type,
            dates=[i["time_value"] for i in response["epidata"] if not np.isnan(i["value"])],
            values=[i["value"] for i in response["epidata"] if not np.isnan(i["value"])]
        )
    for day in missing_dates:
        if not use_latest_issue:
            response = Epidata.covidcast(data_source=sensor.source,
                                         signals=sensor.signal,
                                         time_type="day",
                                         time_values=Epidata.range(20200101, day),
                                         geo_value=ground_truth.geo_value,
                                         geo_type=ground_truth.geo_type,
                                         as_of=day)
            if response["result"] == -2:
                print(f"No indicator data for {day}.")
                continue
            if response["result"] not in (1, -2):
                raise Exception(f"Bad result from Epidata: {response['message']}")
            indicator_values = LocationSeries(
                geo_value=ground_truth.geo_value,
                geo_type=ground_truth.geo_type,
                dates=[i["time_value"] for i in response["epidata"] if not np.isnan(i["value"])],
                values=[i["value"] for i in response["epidata"] if not np.isnan(i["value"])]
            )
        sensor_value = compute_regression_sensor(day, indicator_values, ground_truth)
        if np.isnan(sensor_value):
            continue
        output.add_data(day, sensor_value)  # if np array would need to change append method
        if export_data:
            _export_to_csv(sensor_value, sensor, ground_truth.geo_type, ground_truth.geo_value, day)
    return output


def _get_historical_data(sensor: SignalConfig,
                         geo_type: str,
                         geo_value: str,
                         start_date: int,
                         end_date: int) -> Tuple[LocationSeries, list]:
    """
    Query Epidata API for historical sensorization data.

    Will only return values if they are not null. If they are null or are not available, they will
    be listed as missing.

    Parameters
    ----------
    sensor
        SignalConfig specifying which sensor to retrieve.
    geo_type
        Geo type to retrieve.
    geo_value
        Geo value to retrieve.
    start_date
        First day to retrieve (inclusive).
    end_date
        Last day to retrieve (inclusive).

    Returns
    -------
        Tuple of (LocationSeries containing non-na data, list of dates without valid data)
    """
    ########################################################################################
    # Epidata.covidcast_nowcast not yet published to pypi
    ########################################################################################
    Epidata.BASE_URL = "http://localhost:10080/epidata/api.php"
    response = Epidata.covidcast_nowcast(data_source=sensor.source,
                                         signals=sensor.signal,
                                         time_type="day",
                                         geo_type=geo_type,
                                         time_values=Epidata.range(start_date, end_date),
                                         geo_value=geo_value,
                                         sensor_names=sensor.name)
    Epidata.BASE_URL = "https://delphi.cmu.edu/epidata/api.php"
    if response["result"] == 1:
        output = LocationSeries(
            dates=[i["time_value"] for i in response["epidata"] if not np.isnan(i["value"])],
            values=[i["value"] for i in response["epidata"] if not np.isnan(i["value"])],
            geo_value=geo_value,
            geo_type=geo_type
        )
    elif response["result"] == -2:  # no results
        print("No historical results found")
        output = LocationSeries(geo_value=geo_value, geo_type=geo_type)
    else:
        raise Exception(f"Bad result from Epidata: {response['message']}")

    all_dates = [int(i.strftime("%Y%m%d")) for i in date_range(str(start_date), str(end_date))]
    missing_dates = [i for i in all_dates if i not in output.dates]
    return output, missing_dates


def _export_to_csv(value,
                   sensor,
                   geo_type,
                   geo_value,
                   date,
                   receiving_dir="/common/covidcast_nowcast/receiving"  # convert this to use params file
                   ) -> str:
    """Save value to csv for upload to epidata database"""
    export_dir = os.path.join(receiving_dir, sensor.source)
    os.makedirs(export_dir, exist_ok=True)
    export_file = os.path.join(export_dir, f"{date}_{geo_type}_{sensor.signal}.csv")
    with open(export_file, "w") as f:
        f.write("sensor_name,geo_value,value\n")
        f.write(f"{sensor.name},{geo_value},{value}\n")
    return export_file

