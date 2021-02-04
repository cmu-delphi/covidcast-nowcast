"""Generate ground truth signal."""

import datetime
from typing import Callable, List, Optional, Tuple, Union

import numpy as np
from delphi_epidata import Epidata
from scipy.linalg import toeplitz
from scipy.sparse import diags as band

from ..data_containers import LocationSeries, SensorConfig


def _construct_convolution_matrix(signal: np.ndarray,
                                  kernel: np.ndarray) -> np.ndarray:
    """
    Constructs full convolution matrix (n+m-1) x n,
    where n is the signal length and m the kernel length.

    Parameters
    ----------
    signal
        array of values to convolve
    kernel
        array with convolution kernel values

    Returns
    -------
        convolution matrix
    """
    n = signal.shape[0]
    m = kernel.shape[0]
    padding = np.zeros(n - 1)
    first_col = np.r_[kernel, padding]
    first_row = np.r_[kernel[0], padding]

    return toeplitz(first_col, first_row)


def _fft_convolve(signal: np.ndarray, kernel: np.ndarray) -> np.ndarray:
    """
    Perform 1D convolution in the frequency domain.

    Parameters
    ----------
    signal
        array of values to convolve
    kernel
        array with convolution kernel values

    Returns
    -------
        array with convolved signal values

    """
    n = signal.shape[0]
    m = kernel.shape[0]
    signal_freq = np.fft.fft(signal, n + m - 1)
    kernel_freq = np.fft.fft(kernel, n + m - 1)
    return np.fft.ifft(signal_freq * kernel_freq).real[:n]


def deconvolve_tf(y: np.ndarray,
                  kernel: np.ndarray,
                  lam: float,
                  n_iters: int = 100,
                  k: int = 2,
                  clip: bool = False) -> np.ndarray:
    """
    Perform trend filtering regularized deconvolution through the following optimization

        minimize  (1/2n) ||y - Cx||_2^2 + lam*||D^(k+1)x||_1
            x

    where C is the discrete convolution matrix, and D^(k+1) the discrete differences
    operator. The second term adds a trend filtering (tf) penalty.

    Parameters
    ----------
    y
        array of values to convolve
    kernel
        array with convolution kernel values
    lam
        regularization parameter for trend filtering penalty smoothness
    n_iters
        number of ADMM interations to perform.
    k
        order of the trend filtering penalty.
    clip
        Boolean to clip count values to [0, infty).

    Returns
    -------
        array of the deconvolved signal values
    """

    def _soft_thresh(x: np.ndarray, lam: float) -> np.ndarray:
        """Perform soft-thresholding of x with threshold lam."""
        return np.sign(x) * np.maximum(np.abs(x) - lam, 0)

    n = y.shape[0]
    m = kernel.shape[0]
    rho = lam  # set equal
    C = _construct_convolution_matrix(y, kernel)[:n, ]
    D = band([-1, 1], [0, 1], shape=(n - 1, n)).toarray()
    D = np.diff(D, n=k, axis=0)

    # pre-calculations
    DtD = D.T @ D
    CtC = C.T @ C / n
    Cty = C.T @ y / n
    x_update_1 = np.linalg.inv(CtC + rho * DtD)

    # begin admm loop
    x_k = None
    alpha_0 = np.zeros(n - k - 1)
    u_0 = np.zeros(n - k - 1)
    for t in range(n_iters):
        x_k = x_update_1 @ (Cty + rho * D.T @ (alpha_0 - u_0))
        Dx_u0 = np.diff(x_k, n=(k + 1)) + u_0
        alpha_k = _soft_thresh(Dx_u0, lam / rho)
        u_k = Dx_u0 - alpha_k

        alpha_0 = alpha_k
        u_0 = u_k

    if clip:
        x_k = np.clip(x_k, 0, np.infty)

    return x_k


def deconvolve_tf_cv(y: np.ndarray,
                     kernel: np.ndarray,
                     method: str = "forward",
                     cv_grid: np.ndarray = np.logspace(1, 3.5, 10),
                     n_folds: int = 3,
                     n_iters: int = 100,
                     k: int = 2,
                     clip: bool = True,
                     verbose: bool = False) -> np.ndarray:
    """
    Run cross-validation to tune smoothness over deconvolve_tf(). Two types of CV are
    supported

        - "le3o", which leaves out every third value in training, and imputes the
          missing test value with the average of the neighboring points. The
          n_folds argument is ignored if method="le3o".
        - "forward", which trains on values 1,...,t and predicts the (t+1)th value as
          the fitted value for t. The n_folds argument decides the number of points
          to hold out, and then "walk forward".


    Parameters
    ----------
    y
        array of values to convolve
    kernel
        array with convolution kernel values
    method
        string with one of {"le3o", "forward"} specifying cv type
    cv_grid
        grid of trend filtering penalty values to search over
    n_folds
        number of splits for cv (see above documentation)
    n_iters
        number of ADMM interations to perform.
    k
        order of the trend filtering penalty.
    clip
        Boolean to clip count values to [0, infty)
    verbose
        Boolean whether to print debug statements

    Returns
    -------
        array of the deconvolved signal values
    """

    assert (
        method in {"le3o", "forward"},
        "cv method specified should be one of {'le3o', 'forward'}"
    )

    n = y.shape[0]
    cv_loss = np.zeros((cv_grid.shape[0],))

    if method == "le3o":
        # get test indices for a leave-every-three-out CV.
        cv_test_splits = []
        for i in range(3):
            test_idx = np.zeros((n,), dtype=bool)
            test_idx[i::3] = True
            cv_test_splits.append(test_idx)

        for i, test_split in enumerate(cv_test_splits):
            if verbose: print(f"Fitting fold {i}/{len(cv_test_splits)}")
            for j, reg_par in enumerate(cv_grid):
                x_hat = np.full((n,), np.nan)
                x_hat[~test_split] = deconvolve_tf(y[~test_split], kernel, reg_par,
                                                   n_iters, k, clip)
                x_hat = _impute_with_neighbors(x_hat)
                y_hat = _fft_convolve(x_hat, kernel)
                cv_loss[j] += np.sum((y[test_split] - y_hat[test_split]) ** 2)

    elif method == "forward":
        for i in range(1, n_folds + 1):
            if verbose: print(f"Fitting fold {i}/{n_folds}")
            for j, reg_par in enumerate(cv_grid):
                x_hat = np.full((n - i + 1,), np.nan)
                x_hat[:(n - i)] = deconvolve_tf(y[:(n - i)], kernel,
                                                reg_par, n_iters, k, clip)
                x_hat[-1] = x_hat[-2]
                y_hat = _fft_convolve(x_hat, kernel)
                cv_loss[j] += (y[-1] - y_hat[-1]) ** 2

    lam = cv_grid[np.argmin(cv_loss)]
    if verbose: print(f"Chosen parameter: {lam:.4}")
    x_hat = deconvolve_tf(y, kernel, lam, n_iters, k, clip)
    return x_hat


def _impute_with_neighbors(x: np.ndarray) -> np.ndarray:
    """
    Impute missing values with the average of the elements immediately
    before and after.

    Parameters
    ----------
    x
        signal with missing values

    Returns
    -------
        imputed signal
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


def deconvolve_signal(convolved_truth_indicator: SensorConfig,
                      input_dates: List[int],
                      input_locations: List[Tuple[str, str]],
                      kernel: np.ndarray,
                      fit_func: Callable = deconvolve_tf_cv,
                      ) -> List[LocationSeries]:
    """
    Compute ground truth signal value by deconvolving an indicator with a delay
    distribution.

    The deconvolution function is specified by fit_func, by default
    using a least-squares deconvolution with a trend filtering penalty, chosen
    by walk-forward validation.

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
