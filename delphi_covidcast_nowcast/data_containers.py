from dataclasses import dataclass, field
from typing import List, Union

from numpy import ndarray, nan, nanmean, isnan
from pandas import date_range


@dataclass(frozen=True)
class SignalConfig:
    source: str
    signal: str
    name: str
    lag: int


@dataclass
class LocationSeries:
    geo_value: str = None
    geo_type: str = None
    dates: List[int] = field(default_factory=lambda: [])
    values: Union[List, ndarray] = field(default_factory=lambda: [])

    def __post_init__(self):
        if (self.dates is None and self.values is not None) or \
                (self.dates is not None and self.values is None):
            raise ValueError("Must init with both date and values.")

        if (self.dates is not None and self.values is not None) and \
                (len(self.dates) != len(self.values)):
            raise ValueError("Length of dates and values differs.")

        if len(set(self.dates)) < len(self.dates):
            raise ValueError("Duplicate dates not allowed.")

    def add_data(self, date, value):
        """Append a date and value to existing attributes.

        Safer than appending individually since the two lists can't have different lengths
        """
        object.__setattr__(self, 'dates', self.dates + [date])
        object.__setattr__(self, 'values', self.values + [value])

    @property
    def empty(self):
        return True if (not self.dates and not self.values) else False

    def get_value(self, date: int) -> float:
        """Return value for a given date or nan if not available."""
        try:
            return self.values[self.dates.index(date)]
        except ValueError:
            return nan

    def get_data_range(self,
                       start_date: int,
                       end_date: int,
                       imputation_method: str = None) -> List[float]:
        """
        Return value of LocationSeries between two dates with optional imputation.

        Parameters
        ----------
        start_date
            First day to include in range.
        end_date
            Last day to include in range.
        imputation_method
            Optional type of imputation to conduct. Currently only "mean" is supported.

        Returns
        -------
            List of values, one for each day in the range.
        """
        if start_date < min(self.dates) or end_date > max(self.dates):
            raise ValueError(f"Data range must be within existing dates "
                             f"{min(self.dates)}-{max(self.dates)}.")
        all_dates = [int(i.strftime("%Y%m%d")) for i in date_range(str(start_date), str(end_date))]
        out_values = []
        for day in all_dates:
            out_values.append(self.get_value(day))
        if imputation_method is None or not out_values:
            return out_values
        elif imputation_method == "mean":
            mean = nanmean(out_values)
            out_values = [i if not isnan(i) else mean for i in out_values]
            return out_values
        else:
            raise ValueError("Invalid imputation method. Must be None or 'mean'")
