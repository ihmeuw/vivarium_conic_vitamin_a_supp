"""
=========================
Risk Data Transformations
=========================

This module contains tools for handling raw risk exposure and relative
risk data and performing any necessary data transformations.

"""
from typing import Union

import numpy as np
import pandas as pd

from vivarium.framework.randomness import RandomnessStream
from vivarium_public_health.utilities import EntityString, TargetString
from vivarium_public_health.risks.data_transformations import (
    validate_relative_risk_data_source,
    rebin_relative_risk_data,
    get_distribution_type,
    pivot_categorical,
    _make_relative_risk_data,
    get_exposure_data
)


###############################
# Relative risk data handlers #
###############################

def get_relative_risk_data(builder, risk: EntityString, target: TargetString, randomness: RandomnessStream):
    source_type = validate_relative_risk_data_source(builder, risk, target)
    relative_risk_data = load_relative_risk_data(builder, risk, target, source_type, randomness)
    relative_risk_data = rebin_relative_risk_data(builder, risk, relative_risk_data)

    if get_distribution_type(builder, risk) in ['dichotomous', 'ordered_polytomous', 'unordered_polytomous']:
        relative_risk_data = pivot_categorical(relative_risk_data)

    else:
        relative_risk_data = relative_risk_data.drop(['parameter'], 'columns')

    return relative_risk_data


def load_relative_risk_data(builder, risk: EntityString, target: TargetString,
                            source_type: str, randomness: RandomnessStream):
    relative_risk_source = builder.configuration[f'effect_of_{risk.name}_on_{target.name}'][target.measure]

    if source_type == 'data':
        relative_risk_data = builder.data.load(f'{risk}.relative_risk')
        correct_target = ((relative_risk_data['affected_entity'] == target.name)
                          & (relative_risk_data['affected_measure'] == target.measure))
        relative_risk_data = (relative_risk_data[correct_target]
                              .drop(['affected_entity', 'affected_measure'], 'columns'))

    elif source_type == 'relative risk value':
        relative_risk_data = _make_relative_risk_data(builder, float(relative_risk_source['relative_risk']))

    else:  # distribution
        parameters = {k: v for k, v in relative_risk_source.to_dict().items()  if v is not None}
        random_state = np.random.RandomState(randomness.get_seed())
        cat1_value = generate_relative_risk_from_distribution(random_state, parameters)
        relative_risk_data = _make_relative_risk_data(builder, cat1_value)

    return relative_risk_data


def generate_relative_risk_from_distribution(random_state: np.random.RandomState,
                                             parameters: dict) -> Union[float, pd.Series, np.ndarray]:
    first = pd.Series(list(parameters.values())[0])
    length = len(first)
    index = first.index

    for v in parameters.values():
        if length != len(pd.Series(v)) or not index.equals(pd.Series(v).index):
            raise ValueError('If specifying vectorized parameters, all parameters '
                             'must be the same length and have the same index.')

    if 'mean' in parameters:  # normal distribution
        rr_value = random_state.normal(parameters['mean'], parameters['se'])
    elif 'log_mean' in parameters:  # log distribution
        log_value = parameters['log_mean'] + parameters['log_se']*random_state.randn()
        if parameters['tau_squared']:
            log_value += random_state.normal(0, parameters['tau_squared'])
        rr_value = np.exp(log_value)
    else:
        raise NotImplementedError(f'Only normal distributions (supplying mean and se) and log distributions '
                                  f'(supplying log_mean, log_se, and tau_squared) are currently supported.')

    rr_value = np.maximum(1, rr_value)

    return rr_value

# ################################################
# Population attributable fraction data handlers #
# ################################################
def get_population_attributable_fraction_data(builder, risk: EntityString,
                                              target: TargetString, randomness: RandomnessStream):
    exposure_source = builder.configuration[f'{risk.name}']['exposure']
    rr_source_type = validate_relative_risk_data_source(builder, risk, target)

    if exposure_source == 'data' and rr_source_type == 'data' and risk.type == 'risk_factor':
        paf_data = builder.data.load(f'{risk}.population_attributable_fraction')
        correct_target = ((paf_data['affected_entity'] == target.name)
                          & (paf_data['affected_measure'] == target.measure))
        paf_data = (paf_data[correct_target]
                    .drop(['affected_entity', 'affected_measure'], 'columns'))
    else:
        key_cols = ['sex', 'age_start', 'age_end', 'year_start', 'year_end']
        exposure_data = get_exposure_data(builder, risk).set_index(key_cols)
        relative_risk_data = get_relative_risk_data(builder, risk, target, randomness).set_index(key_cols)
        mean_rr = (exposure_data * relative_risk_data).sum(axis=1)
        paf_data = ((mean_rr - 1)/mean_rr).reset_index().rename(columns={0: 'value'})
    return paf_data
