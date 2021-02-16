# process-agnostic

Estimating true infecteds from the data we have is a Ridiculously Hard problem.
Further, since ground truth of true infecteds is latent, and we only see
partial outcomes like cases and deaths, the only way to validate a model is by
making predictions about the future.

The idea behind this approach is to decouple the problem stated above into two
smaller problems:

1. given sporatic, laggy, and preliminary data, predict the final version _of
the same data_ at the current time. In other words, estimate the data streams
directly, without any modeling of the driving processes.
2. estimate, for example, time-varying SIR model parameters given stable,
up-to-date data from above

These are both still hard problems, but by approaching them separately we have
more flexibility in methodology. One of the biggest benefits of this approach
is that the first problem becomes trivially back-testable in the sense that we
can immediately validate attempts with respect to historical data. The second
problem still must be validated through forecasting, but it is made
incrementally easier by having stable, up-to-date data from all sources.

# nowcasting and forecasting are distinct tasks

In going through this exercise I had something of an epiphany regarding the
tasks of nowcasting and forecasting. The realization I had in the attached
notebook is that what I've been doing so far is actually perhaps more correctly
described as _short-term forecasting_ rather than _nowcasting_.

These ideas have been somewhat convoluted in my mind in the past, but they're
beginning to solidify now. Here I posit that there is a fundamental distinction
between the tasks of nowcasting and forecasting, and maybe this will help frame
our thinking.

## definitions

I've often heard nowcasts described as forecasts of the present, and I've been
guilty myself of similar descriptions. In fact, for a long time I loosely
though of predictions of the present as nowcasts, predictions of the future as
forecasts, and predictions of the past as backcasts. I now argue against that
definition and claim that what separates a nowcast from a forecast or a
backcast is _not_ the timing of the prediction (i.e. past, present, or future),
but is instead something inherent in structure of the task.

I propose the following definitions:

- A **nowcast** is an estimate of a target signal based on _separate but
contemporaneous_ signals, all at some instant in time.
- A **forecast** is an estimate of the _future value_ of a target signal, based
on the target signal and other signals, all of which are available only in the
past relative to the prediction.
- A **backcast** is an estimate of the _past value_ of a target signal, based
on the target signal and other signals, all of which are available at any time,
past or future, relative to the prediction.
- And for completeness, a **retrospective forecast** is identical to a
forecast, except that we pretend to turn back the clock such that the _future
value_ is in reality already in the past. Critically, this means that we
withhold any information which was not known at the point in time in the past
when we pretend that the forecast was produced.

Following from the above, forecasting and retrospective forecasting are
identical tasks, modulo the definition of the present. Forecasting and
backcasting are highly similar tasks, with the distinction that backcasts may
incorporate future data, which is explicitly not the case for forecasts.

Nowcasting is fundamentally different from the other tasks in the sense that it
is done in-place with respect to time. Whereas forecasts and backcasts are
parameterized by a pair of instants in time (the point in time when the
prediction is made, and the point in time being predicted), a nowcast is
parameterized by just a single instant: "now".

As an aside, we can frame the Kalman filter under this terminology. It is
widely described as a two-step process: "predict" and "update". The "predict"
step performs a _forecast_ (namely, autoregression), and the "update" step
performs a _nowcast_ (namely, sensor fusion).

## flu example

In the flu setting, we have weekly data. The target signal, weighted
Influenza-like illness (wILI), is lagged by several days to a week, or more,
and is subject to backfill over subsequent months. However, we also have stable
digital surveillance signals in quasi-real-time from sources like Wikipedia,
HealthTweets, CDC, and Google Trends.

The task is to combine the weekly value of each digital surveillance source
into a single, unified estimate of wILI for the present week. This is a classic
instance of nowcasting.

Note that in practice we actually also take forecasts (e.g. "SAR3") and feed
them into the fusion as if they were bona fide surveillance signals. So in a
practical sense, the line between nowcasting and forecating may be somewhat
blurred -- and that's ok.

## application to covid

In contrast with the flu setting, here in the covid setting I found, in the
strictest sense, a classic instance of _forecasting_. I'm predicting the daily
value of some target signal (maybe cases, or deaths, or infecteds, etc), based
on other daily signals that, from what I can tell, aren't actually available in
real-time at daily resolution. At best, we have a lag of one day (e.g. online
surveys); at worst, the lag is on the order of weeks (e.g. clinical results).

In other words, I'm predicting data on today, `t`, given data from days `t-1`
and before -- a forecast.

This doesn't necessarily preclude loosely using the term "nowcasting", nor does
it preclude use of methods like sensor fusion. But thinking about the problem
in terms of the definitions above helps me to better understand and communicate
the challenges and approaches.
