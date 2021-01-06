"""Run nowcast."""

import datetime
from typing import List, Optional, Tuple, Union

import numpy as np

import src.deconvolution as deconv
import src.sensors as sensors

def nowcast(input_dates: List[int],
            input_locations: List[Tuple[str, str]],
            sensor_indicators: List[Tuple[str, str]],
            convolved_truth_indicator: Tuple[str, str],
            kernel: List[float],
            nowcast_dates: List[int] = "*",
            ) -> Tuple[np.ndarray, np.ndarray, List]:
    """

    Parameters
    ----------
    input_dates
        List of dates to train data on and get nowcasts for.
    input_location
        List of (location, geo_type) tuples specifying locations to train and obtain nowcasts for.
    sensor_indicators
        List of (source, signal) tuples specifying indicators to use as sensors.
    convolved_truth_indicator
        (source, signal) tuple of quantity to deconvolve.
    kernel
        Delay distribution to deconvolve with convolved_truth_indicator
    nowcast_dates
        Dates to get predictions for. Defaults to input_dates + additional day.

    Returns
    -------
        (predicted values, std devs, locations)
    """
    # get geo mappings

    # deconvolve for ground truth
    ground_truth = deconv.deconvolve_signal(convolved_truth_indicator, input_dates,
                                            input_locations, np.array(kernel))

    # fit sensors
    # generate statespace
    # estimate covariance
    # run SF
    # return output
    pass