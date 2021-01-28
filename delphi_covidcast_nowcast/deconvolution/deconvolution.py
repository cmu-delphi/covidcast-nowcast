"""Generate ground truth signal."""

import datetime
from typing import Callable, List, Optional, Tuple, Union

import numpy as np
from delphi_epidata import Epidata
from scipy.linalg import toeplitz
from scipy.sparse import diags as band

from ..data_containers import LocationSeries, SensorConfig


class TempEpidata:

    @staticmethod
    def to_date(date: Union[int, str], fmt: str = '%Y%m%d') -> datetime.date:
        return datetime.datetime.strptime(str(date), fmt).date()

    @staticmethod
    def get_signal_range(source: str, signal: str, start_date: int, end_date: int,
                         geo_type: str, geo_value: Union[int, str, float]
                         ) -> Optional[LocationSeries]:
        response = Epidata.covidcast(source, signal, 'day', geo_type,
                                     Epidata.range(start_date, end_date),
                                     geo_value)
        if response['result'] != 1:
            print(f'api returned {response["result"]}: {response["message"]}')
            return None
        values = [(row['time_value'], row['value']) for row in response['epidata']]
        values = sorted(values, key=lambda ab: ab[0])
        return LocationSeries(geo_value, geo_type,
                              [ab[0] for ab in values],
                              np.array([ab[1] for ab in values]))


class Deconvolution:

    @staticmethod
    def get_convolution_matrix(signal: np.ndarray, kernel: np.ndarray) -> np.ndarray:
        """
        Constructs full convolution matrix (n+m-1) x n,
        where n is the signal length and m the kernel length.

        Parameters
        ----------
        signal
            Signal to convolve.
        kernel
            Convolution kernel.

        Returns
        -------
            Convolution matrix.
        """
        n = signal.shape[0]
        m = kernel.shape[0]
        padding = np.zeros(n - 1)
        first_col = np.r_[kernel, padding]
        first_row = np.r_[kernel[0], padding]

        return toeplitz(first_col, first_row)

    @staticmethod
    def freq_convolve(signal: np.ndarray, kernel: np.ndarray) -> np.ndarray:
        """1D convolution in the frequency domain"""
        n = signal.shape[0]
        m = kernel.shape[0]
        signal_freq = np.fft.fft(signal, n + m - 1)
        kernel_freq = np.fft.fft(kernel, n + m - 1)
        return np.fft.ifft(signal_freq * kernel_freq).real[:n]

    @staticmethod
    def soft_thresh(x: np.ndarray, lam: float) -> np.ndarray:
        """Perform soft-thresholding of x with threshold lam."""
        return np.sign(x) * np.maximum(np.abs(x) - lam, 0)

    @staticmethod
    def fit(y: np.ndarray, kernel: np.ndarray, lam: float, n_iters: int = 100, k: int = 2,
            clip: bool = True) -> np.ndarray:
        """
        Solve the following optimization problem via ADMM

        minimize  (1/2n) ||y - Cx||_2^2 + lam*||D^(k+1)x||_1
            x

        where C is the discrete convolution matrix, and D^(k+1)
        the discrete differences operator.

        Parameters
        ----------
        y
            Signal to deconvolve.
        kernel
            Convolution filter.
        lam
            Regularization parameter for smoothness.
        n_iters
            Number of ADMM interations to perform.
        k
            Order of the trend filtering penalty.
        clip
            Boolean to clip count values to [0, infty).

        Returns
        -------
            array of the deconvolved signal
        """
        n = y.shape[0]
        m = kernel.shape[0]
        rho = lam  # set equal
        C = Deconvolution.get_convolution_matrix(y, kernel)[:n, ]
        D = band([-1, 1], [0, 1], shape=(n - 1, n)).toarray()
        D = np.diff(D, n=k, axis=0)

        # pre-calculations
        DtD = D.T @ D
        CtC = C.T @ C / n
        Cty = C.T @ y / n
        x_update_1 = np.linalg.inv(CtC + rho * DtD)

        x_k = None
        alpha_0 = np.zeros(n - k - 1)
        u_0 = np.zeros(n - k - 1)
        for t in range(n_iters):
            x_k = x_update_1 @ (Cty + rho * D.T @ (alpha_0 - u_0))
            Dx_u0 = np.diff(x_k, n=(k + 1)) + u_0
            alpha_k = Deconvolution.soft_thresh(Dx_u0, lam / rho)
            u_k = Dx_u0 - alpha_k

            alpha_0 = alpha_k
            u_0 = u_k

        if clip:
            x_k = np.clip(x_k, 0, np.infty)

        return x_k

    @staticmethod
    def fit_cv_le3o(y: np.ndarray, kernel: np.ndarray,
                    cv_grid: np.ndarray = np.logspace(1, 3.5, 10),
                    k: int = 2, clip: bool = True, verbose: bool = False) -> np.ndarray:
        n = y.shape[0]
        cv_test_splits = CrossValidation.le3o_inds(y)
        cv_loss = np.zeros((cv_grid.shape[0],))
        for i, test_split in enumerate(cv_test_splits):
            if verbose: print(f"Fitting fold {i}/{len(cv_test_splits)}")
            for j, reg_par in enumerate(cv_grid):
                x_hat = np.full((n,), np.nan)
                x_hat[~test_split] = Deconvolution.fit(y[~test_split], kernel,
                                                       reg_par, k=k, clip=clip)
                x_hat = CrossValidation.impute_missing(x_hat)
                y_hat = Deconvolution.freq_convolve(x_hat, kernel)
                cv_loss[j] += np.sum((y[test_split] - y_hat[test_split]) ** 2)

        lam = cv_grid[np.argmin(cv_loss)]
        if verbose: print(f"Chosen parameter: {lam:.4}")
        x_hat = Deconvolution.fit(y, kernel, lam, k=k)
        return x_hat

    @staticmethod
    def fit_cv_forward(y: np.ndarray, kernel: np.ndarray,
                       cv_grid: np.ndarray = np.logspace(1, 3.5, 10),
                       k: int = 2, n_folds: int = 5,
                       clip: bool = True, verbose: bool = False) -> np.ndarray:
        n = y.shape[0]
        cv_loss = np.zeros((cv_grid.shape[0],))
        for i in range(1, n_folds + 1):
            if verbose: print(f"Fitting fold {i}/{n_folds}")
            for j, reg_par in enumerate(cv_grid):
                x_hat = np.full((n - i + 1,), np.nan)
                x_hat[:(n - i)] = Deconvolution.fit(y[:(n - i)], kernel,
                                                    reg_par, k=k, clip=clip)
                x_hat[-1] = x_hat[-2]
                y_hat = Deconvolution.freq_convolve(x_hat, kernel)
                cv_loss[j] += (y[-1] - y_hat[-1]) ** 2

        lam = cv_grid[np.argmin(cv_loss)]
        if verbose: print(f"Chosen parameter: {lam:.4}")
        x_hat = Deconvolution.fit(y, kernel, lam, k=k)
        return x_hat


class CrossValidation:

    @staticmethod
    def le3o_inds(x: np.ndarray) -> List[np.ndarray]:
        """
        Get test indices for a leave-every-three-out CV.

        Returns a list of 3 boolean arrays. The first array,
        arr1, corresponding to the first split, is used:
          * x[arr1] slices the test rows
          * x[~arr1] slices the train rows
        Similarly for the other two splits.

        """
        m = 3
        n = x.shape[0]

        test_idxs = []
        for i in range(m):
            test_idx = np.zeros((n,), dtype=bool)
            test_idx[i::m] = True
            test_idxs.append(test_idx)

        return test_idxs

    @staticmethod
    def impute_missing(x: np.ndarray) -> np.ndarray:
        """
        Impute missing values with the average of the
        elements immediate before and after.
        """
        # handle edges
        if np.isnan(x[0]):
            x[0] = x[1]

        if np.isnan(x[-1]):
            x[-1] = x[-2]

        imputed_x = np.copy(x)
        for i, (a, b, c) in enumerate(zip(x, x[1:], x[2:])):
            if np.isnan(b):
                imputed_x[i + 1] = (a + c) / 2

        assert np.isnan(imputed_x).sum() == 0

        return imputed_x


def deconvolve_signal(convolved_truth_indicator: SensorConfig,
                      input_dates: List[int],
                      input_locations: List[Tuple[str, str]],
                      kernel: np.ndarray,
                      fit_func: Callable = Deconvolution.fit_cv_forward,
                      ) -> List[LocationSeries]:
    """
    Compute ground truth signal value by deconvolving an indicator with a delay distribution.

    A trend filtering penalty is applied to smooth the output.

    Parameters
    ----------
    convolved_truth_indicator
        (source, signal) tuple of quantity to deconvolve.
    input_dates
        List of dates to train data on and get nowcasts for.
    input_locations
        List of (location, geo_type) tuples specifying locations to train and obtain nowcasts for.
    kernel
        Delay distribution from infection to report.
    fit_func
        Fitting function for the deconvolution.

    Returns
    -------
        dataclass with deconvolved signal and corresponding location/dates
    """

    n_locs = len(input_locations)

    # full date range (input_dates can be discontinuous)
    start_date = TempEpidata.to_date(input_dates[0])
    end_date = TempEpidata.to_date(input_dates[-1])
    n_full_dates = (end_date - start_date).days + 1
    full_dates = [start_date + datetime.timedelta(days=a) for a in range(n_full_dates)]
    full_dates = [int(d.strftime('%Y%m%d')) for d in full_dates]

    # output corresponds to order of input_locations
    deconvolved_truth = []
    for j, (loc, geo_type) in enumerate(input_locations):
        # epidata call to get convolved truth
        # note: returns signal over input dates, continuous. addtl filtering needed if
        # input dates is not continuous/missing dates. We can't filter here, because
        # deconvolution requires a complete time series.
        convolved_truth = TempEpidata.get_signal_range(convolved_truth_indicator.source,
                                                       convolved_truth_indicator.signal,
                                                       input_dates[0], input_dates[-1],
                                                       geo_type, loc)

        # todo: better handle missing dates/locations
        if convolved_truth is not None:
            deconvolved_truth.append(LocationSeries(loc, geo_type, convolved_truth.dates,
                                                    fit_func(
                                                        convolved_truth.values,
                                                        kernel)
                                                    ))
        else:
            deconvolved_truth.append(LocationSeries(loc, geo_type, full_dates,
                                                    np.full((n_full_dates,), np.nan)))

        if (j + 1) % 25 == 0: print(f"Deconvolved {j}/{n_locs}")

    # filter for desired input dates
    # input_idx = [i for i, date in enumerate(full_dates) if date in input_dates]
    # deconvolved_truth = deconvolved_truth[input_idx, :]

    return deconvolved_truth
