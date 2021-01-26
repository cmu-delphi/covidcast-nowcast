import os
import tempfile
from unittest.mock import patch

import numpy as np

from delphi_covidcast_nowcast.data_containers import LocationSeries, SignalConfig
from delphi_covidcast_nowcast.sensorization.sensor import \
    historical_sensors, get_regression_sensor_values, get_ar_sensor_values, \
    get_historical_sensor_data, _export_to_csv


class TestGetSensors:

    @patch("delphi_covidcast_nowcast.sensorization.sensor.get_ar_sensor_values")
    @patch("delphi_covidcast_nowcast.sensorization.sensor.get_regression_sensor_values")
    def test_get_sensors(self, get_regression_sensor_values, get_ar_sensor_values):
        """Test sensors are obtained correctly."""
        get_regression_sensor_values.side_effect = [LocationSeries("w", values=[1], dates=[1]),
                                                    LocationSeries("x", values=[1], dates=[1]),
                                                    LocationSeries("y"),
                                                    LocationSeries("z", values=[1], dates=[1])]
        get_ar_sensor_values.side_effect = [LocationSeries("i", values=[1], dates=[1]),
                                            LocationSeries("j")]
        test_sensors = [SignalConfig("src1", "sigA"),
                        SignalConfig("src2", "sigB")]
        test_ground_truths = [
            LocationSeries(geo_value="pa", geo_type="state", values=[2, 3], dates=[0, 1]),
            LocationSeries(geo_value="ak", geo_type="state", values=[4, 5], dates=[0, 1])]
        assert historical_sensors(None, None, test_sensors, test_ground_truths, True, True, False) == {
            "ground_truth_ar": [LocationSeries("i", values=[1], dates=[1])],
            SignalConfig("src1", "sigA"): [LocationSeries("w", values=[1], dates=[1])],
            SignalConfig("src2", "sigB"): [LocationSeries("x", values=[1], dates=[1]),
                                           LocationSeries("z", values=[1], dates=[1])]
        }

    @patch("delphi_covidcast_nowcast.sensorization.sensor.get_ar_sensor_values")
    def test_get_sensors_ar_only(self, get_ar_sensor_values):
        """Test that not passing in regression sensors still works"""
        get_ar_sensor_values.side_effect = [LocationSeries("i", values=[1], dates=[1]),
                                            LocationSeries("j", values=[1], dates=[1]),
                                            LocationSeries("k")]
        test_ground_truths = [
            LocationSeries(geo_value="pa", geo_type="state", values=[2, 3], dates=[0, 1]),
            LocationSeries(geo_value="ak", geo_type="state", values=[4, 5], dates=[0, 1])]
        assert historical_sensors(None, None, [], test_ground_truths, True, True, False) == {
            "ground_truth_ar": [LocationSeries("i", values=[1], dates=[1]),
                                LocationSeries("j", values=[1], dates=[1])],
        }


class TestGetARSensorValues:

    @patch("delphi_covidcast_nowcast.sensorization.sensor.compute_ar_sensor")
    def test_get_regression_sensor_values_no_missing(self, compute_ar_sensor):
        compute_ar_sensor.side_effect = [np.nan, 1.0]
        """Test output returned and nan dates skipped"""
        test_ground_truth = LocationSeries(geo_value="ca", geo_type="state")
        assert get_ar_sensor_values(test_ground_truth, 20200101, 20200102) == \
               LocationSeries(geo_value="ca", geo_type="state", dates=[20200102], values=[1.0])


class TestGetRegressionSensorValues:

    @patch("delphi_covidcast_nowcast.sensorization.sensor.get_historical_sensor_data")
    def test_get_regression_sensor_values_no_missing(self, historical):
        """Test output is just returned if no missing dates"""
        historical.return_value = ("output", [])
        test_ground_truth = LocationSeries(geo_value="ca", geo_type="state")
        assert get_regression_sensor_values(
            None, None, None, test_ground_truth, True, True, False) == "output"


    @patch("delphi_covidcast_nowcast.sensorization.sensor.get_indicator_data")
    @patch("delphi_covidcast_nowcast.sensorization.sensor.get_historical_sensor_data")
    def test_get_regression_sensor_values_no_results(self, historical, get_indicator_data):
        """Test output is just returned in covidcast API has no results"""
        historical.return_value = ("output", [20200101])
        get_indicator_data.return_value = {}
        regression_sensors = SignalConfig(source="test", signal="test")
        test_ground_truth = LocationSeries(geo_value="ca", geo_type="state")
        assert get_regression_sensor_values(regression_sensors, None, None, test_ground_truth, True,
                                            True, False) == "output"

    @patch("delphi_covidcast_nowcast.sensorization.sensor.get_indicator_data")
    @patch("delphi_covidcast_nowcast.sensorization.sensor.compute_regression_sensor")
    @patch("delphi_covidcast_nowcast.sensorization.sensor.get_historical_sensor_data")
    def test_get_regression_sensor_values_compute_as_of(self, historical, compute_regression_sensor, get_indicator_data):
        """Test computation functions are called for missing dates"""
        historical.return_value = (LocationSeries(values=[], dates=[]), [20200101, 20200102])
        compute_regression_sensor.side_effect = [np.nan, 1.0]
        get_indicator_data.return_value = {20200101: {"result": 1, "epidata": [{"time_value": 0, "value": 0}]},
                                           20200102: {"result": 1, "epidata": [{"time_value": 0, "value": 0}]}}
        test_ground_truth = LocationSeries(geo_value="ca", geo_type="state")

        regression_sensors = SignalConfig()
        assert get_regression_sensor_values(
            regression_sensors, None, None, test_ground_truth, True, False, False) == \
               LocationSeries(values=[1.0], dates=[20200102])


class TestExportToCSV:

    def test__export_to_csv(self):
        test_sensor = SignalConfig(source="src",
                                   signal="sig",
                                   name="test",
                                   lag=4)
        with tempfile.TemporaryDirectory() as tmpdir:
            out_file = _export_to_csv(1.5, test_sensor, "state", "ca", receiving_dir=tmpdir)
            assert os.path.isfile(out_file)
            with open(out_file) as f:
                assert f.read() == "sensor_name,geo_value,value,lag\ntest,ca,1.5,4\n"
