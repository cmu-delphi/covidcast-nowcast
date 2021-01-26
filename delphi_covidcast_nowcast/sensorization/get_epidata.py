import asyncio
from typing import Tuple, List, Dict
from itertools import product
from collections import defaultdict

from numpy import isnan
from pandas import date_range
from aiohttp import ClientSession

# from ..delphi_epidata import Epidata  # used for local testing
from delphi_epidata import Epidata

from ..data_containers import LocationSeries, SignalConfig


async def get(params, session, sensor, location):
    async with session.get(Epidata.BASE_URL, params=params) as response:
        return await response.json(), sensor, location


async def fetch_epidata(combos, as_of):
    tasks = []
    async with ClientSession() as session:
        for sensor, location in combos:
            params = {
                    "endpoint": "covidcast",
                    "data_source": sensor.source,
                    "signals": sensor.signal,
                    "time_type": "day",
                    "geo_type": location.geo_type,
                    "time_values": f"20200101-{as_of}",
                    "geo_value": location.geo_value,
                    "as_of": as_of
                }
            task = asyncio.ensure_future(get(params, session, sensor, location))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses


def get_indicator_data(sensors: List[SignalConfig],
                       locations: List[LocationSeries],
                       as_of: int) -> Dict[Tuple, LocationSeries]:
    # gets all available data up to as_of day for now, could be optimized to only get a window
    output = {}
    all_combos = product(sensors, locations)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(fetch_epidata(all_combos, as_of))
    responses = loop.run_until_complete(future)
    for response, sensor, location in responses:
        if response["result"] not in (-2, 1):
            raise Exception(f"Bad result from Epidata: {response['message']}")
        data = LocationSeries(
            geo_value=location.geo_value,
            geo_type=location.geo_type,
            dates=[i["time_value"] for i in response.get("epidata", []) if not isnan(i["value"])],
            values=[i["value"] for i in response.get("epidata", []) if not isnan(i["value"])]
        )
        if not data.empty:  # TODO OR NOT ENOUGH RESPONSES
            output[(sensor.source, sensor.signal, location.geo_type, location.geo_value)] = data
    return output


def get_historical_sensor_data(sensor: SignalConfig,
                               geo_value: str,
                               geo_type: str,
                               end_date: int,
                               start_date: int) -> Tuple[LocationSeries, list]:
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
                                         sensor_names=sensor.name,
                                         lag=sensor.lag)
    Epidata.BASE_URL = "https://delphi.cmu.edu/epidata/api.php"
    if response["result"] == 1:
        output = LocationSeries(
            dates=[i["time_value"] for i in response["epidata"] if not isnan(i["value"])],
            values=[i["value"] for i in response["epidata"] if not isnan(i["value"])],
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

