"""
=================================
Risk Exposure Distribution Models
=================================

This module contains tools for modeling several different risk
exposure distributions.

"""
import numpy as np
import pandas as pd

from risk_distributions import EnsembleDistribution, Normal, LogNormal

from vivarium.framework.values import list_combiner, union_post_processor
from vivarium_public_health.risks.data_transformations import get_distribution_data
from vivarium_public_health.risks.distributions import EnsembleSimulation, ContinuousDistribution, PolytomousDistribution


class MissingDataError(Exception):
    pass


# FIXME: This is a hack.  It's wrapping up an adaptor pattern in another
# adaptor pattern, which is gross, but would require some more difficult
# refactoring which is thorougly out of scope right now. -J.C. 8/25/19
class SimulationDistribution:
    """Wrapper around a variety of distribution implementations."""

    def __init__(self, risk):
        self.risk = risk

    @property
    def name(self):
        return f'{self.risk}.exposure_distribution'

    def setup(self, builder):
        distribution_data = get_distribution_data(builder, self.risk)
        self.implementation = get_distribution(self.risk, **distribution_data)
        self.implementation.setup(builder)

    def ppf(self, q):
        return self.implementation.ppf(q)

    def __repr__(self):
        return f'ExposureDistribution({self.risk})'


class DichotomousDistribution:
    def __init__(self, risk: str, exposure_data: pd.DataFrame):
        self.risk = risk.name
        self.exposure_data = exposure_data.drop('cat2', axis=1)

    @property
    def name(self):
        return f'dichotomous_distribution.{self.risk}'

    def setup(self, builder):
        self._base_exposure = builder.lookup.build_table(self.exposure_data, key_columns=['sex'],
                                                         parameter_columns=['age', 'year'])
        self.exposure_proportion = builder.value.register_value_producer(f'{self.risk}.exposure_parameters',
                                                                         source=self.exposure)
        base_paf = builder.lookup.build_table(0)
        self.joint_paf = builder.value.register_value_producer(f'{self.risk}.exposure_parameters.paf',
                                                               source=lambda index: [base_paf(index)],
                                                               preferred_combiner=list_combiner,
                                                               preferred_post_processor=union_post_processor)

    def exposure(self, index):
        base_exposure = self._base_exposure(index).values
        joint_paf = self.joint_paf(index).values
        return pd.Series(base_exposure * (1-joint_paf), index=index, name='values')

    def ppf(self, x):
        exposed = x < self.exposure_proportion(x.index)
        return pd.Series(exposed.replace({True: 'cat1', False: 'cat2'}), name=self.risk + '_exposure', index=x.index)

    def __repr__(self):
        return f"DichotomousDistribution(risk={self.risk})"


def get_distribution(risk, distribution_type, exposure, exposure_standard_deviation, weights):
    if distribution_type == 'dichotomous':
        distribution = DichotomousDistribution(risk, exposure)
    elif 'polytomous' in distribution_type:
        distribution = PolytomousDistribution(risk, exposure)
    elif distribution_type == 'normal':
        distribution = ContinuousDistribution(risk, mean=exposure, sd=exposure_standard_deviation,
                                              distribution=Normal)
    elif distribution_type == 'lognormal':
        distribution = ContinuousDistribution(risk, mean=exposure, sd=exposure_standard_deviation,
                                              distribution=LogNormal)
    elif distribution_type == 'ensemble':
        distribution = EnsembleSimulation(risk, weights, mean=exposure, sd=exposure_standard_deviation,)
    else:
        raise NotImplementedError(f"Unhandled distribution type {distribution_type}")
    return distribution
