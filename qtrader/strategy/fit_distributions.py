# -*- coding: utf-8 -*-
# @Time    : 10/2/2021 5:42 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: fit_distributions.py
# @Software: PyCharm

import warnings
import numpy as np
import pandas as pd
import scipy.stats as st
import statsmodels.api as sm
import operator
import matplotlib
import matplotlib.pyplot as plt
from tqdm import tqdm

matplotlib.rcParams['figure.figsize'] = (16.0, 12.0)
matplotlib.style.use('ggplot')

# Distributions to check
DISTRIBUTIONS = [
    st.alpha, st.anglit, st.arcsine, st.beta, st.betaprime, st.bradford, st.burr, st.cauchy, st.chi, st.chi2, st.cosine,
    st.dgamma, st.dweibull, st.erlang, st.expon, st.exponnorm, st.exponweib, st.exponpow, st.f, st.fatiguelife, st.fisk,
    st.foldcauchy, st.foldnorm,
    # st.frechet_r, st.frechet_l,
    st.genlogistic, st.genpareto, st.gennorm, st.genexpon,
    st.genextreme, st.gausshyper, st.gamma, st.gengamma, st.genhalflogistic, st.gilbrat, st.gompertz, st.gumbel_r,
    st.gumbel_l, st.halfcauchy, st.halflogistic, st.halfnorm, st.halfgennorm, st.hypsecant, st.invgamma, st.invgauss,
    st.invweibull, st.johnsonsb, st.johnsonsu, st.ksone, st.kstwobign, st.laplace, st.levy, st.levy_l, st.levy_stable,
    st.logistic, st.loggamma, st.loglaplace, st.lognorm, st.lomax, st.maxwell, st.mielke, st.nakagami, st.ncx2, st.ncf,
    st.nct, st.norm, st.pareto, st.pearson3, st.powerlaw, st.powerlognorm, st.powernorm, st.rdist, st.reciprocal,
    st.rayleigh, st.rice, st.recipinvgauss, st.semicircular, st.t, st.triang, st.truncexpon, st.truncnorm,
    st.tukeylambda,
    st.uniform, st.vonmises, st.vonmises_line, st.wald, st.weibull_min, st.weibull_max, st.wrapcauchy
]

# Create models from data
def best_fit_distribution(data, bins=200, ax=None, distributions=DISTRIBUTIONS):
    """Model data by finding best fit distribution to data"""
    # Get histogram of original data
    y, x = np.histogram(data, bins=bins, density=True)
    x = (x + np.roll(x, -1))[:-1] / 2.0



    # Best holders
    best_distribution = st.norm
    best_params = (0.0, 1.0)
    best_sse = np.inf

    sse_scores = {}
    fit_params = {}

    # Estimate distribution parameters from data
    for distribution in tqdm(distributions):

        # Try to fit the distribution
        try:
            # Ignore warnings from data that can't be fit
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')

                # fit dist to data
                params = distribution.fit(data)

                # Separate parts of parameters
                arg = params[:-2]
                loc = params[-2]
                scale = params[-1]

                # Calculate fitted PDF and error with fit in distribution
                pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
                sse = np.sum(np.power(y - pdf, 2.0))
                sse_scores[distribution.name] = sse
                fit_params[distribution.name] = params

                # if axis pass in add to plot
                try:
                    if ax:
                        pd.Series(pdf, x).plot(ax=ax)
                    # end
                except Exception:
                    pass

                # identify if this distribution is better
                if best_sse > sse > 0:
                    best_distribution = distribution
                    best_params = params
                    best_sse = sse

        except Exception:
            pass

    return (best_distribution.name, best_params, sse_scores, fit_params)

def make_pdf(dist, params, size=10000):
    """Generate distributions's Probability Distribution Function """

    # Separate parts of parameters
    arg = params[:-2]
    loc = params[-2]
    scale = params[-1]

    # Get sane start and end points of distribution
    start = dist.ppf(0.01, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.01, loc=loc, scale=scale)
    end = dist.ppf(0.99, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.99, loc=loc, scale=scale)

    # Build PDF and turn into pandas Series
    x = np.linspace(start, end, size)
    y = dist.pdf(x, loc=loc, scale=scale, *arg)
    pdf = pd.Series(y, x)

    return pdf

# # Load data from statsmodels datasets
# data = pd.Series(sm.datasets.elnino.load_pandas().data.set_index('YEAR').values.ravel())


def plot_fit_distributions(data:pd.Series, fit_names, fit_params):
    # Plot for comparison
    plt.figure(figsize=(12,8))
    ax = data.plot(kind='hist', bins=50, density=True, alpha=0.5)
    # Save plot limits
    dataYLim = ax.get_ylim()

    for fit_name in fit_names:
        best_dist = getattr(st, fit_name)
        best_fit_params = fit_params.get(fit_name)

        # Update plots
        ax.set_ylim(dataYLim)
        ax.set_title(u'All Fitted Distributions')
        ax.set_xlabel(u'Data')
        ax.set_ylabel('Frequency')

        # Make PDF with best params
        pdf = make_pdf(best_dist, best_fit_params)

        param_names = (best_dist.shapes + ', loc, scale').split(', ') if best_dist.shapes else ['loc', 'scale']
        param_str = ', '.join(['{}={:0.2f}'.format(k,v) for k,v in zip(param_names, best_fit_params)])
        dist_str = '{}({})'.format(fit_name, param_str)

        # Display
        ax = pdf.plot(lw=2, label=dist_str, legend=True)
        data.plot(kind='hist', bins=50, density=True, alpha=0.5, ax=ax)

    ax.set_title(u'Fit distributions')
    ax.set_xlabel(u'Data')
    ax.set_ylabel('Frequency')
    plt.show()
