"""Small example."""

import delphi_covidcast_nowcast.deconvolution.delay_kernel as delay
import delphi_covidcast_nowcast.nowcast as nowcast
from delphi_covidcast_nowcast.data_containers import SignalConfig


def main():
    input_dates = list(range(20200601, 20200615))
    input_locations = [('pa', 'state'),
                       ('42003', 'county'),
                       ('42005', 'county'),
                       ('42101', 'county')]
    sensor_indicators = [SignalConfig('usa-facts', 'confirmed_incidence_num', 'ar3'),
                         SignalConfig('fb-survey', 'smoothed_hh_cmnty_cli', 'fb')]
    convolved_truth_indicator = SignalConfig(source='usa-facts',
                                             signal='confirmed_incidence_num')
    kernel = delay.get_florida_delay_distribution()
    nowcast_dates = [20200615]
    use_latest_issue = True

    infections = nowcast.nowcast(input_dates, input_locations,
                                 sensor_indicators, convolved_truth_indicator,
                                 kernel, nowcast_dates, use_latest_issue)


if __name__ == '__main__':
    main()
