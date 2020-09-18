"""
==================
Risk Effect Models
==================

This module contains tools for modeling the relationship between risk
exposure models and disease models.

"""

from vivarium_public_health.risks.effect import RiskEffect as RiskEffect_

from .data_transformations import (
    get_relative_risk_data,
    get_population_attributable_fraction_data
)


class RiskEffect(RiskEffect_):
    """A component to model the impact of a risk factor on the target rate of
    some affected entity. This component can source data either from
    builder.data or from parameters supplied in the configuration.
    For a risk named 'risk' that affects 'affected_risk' and 'affected_cause',
    the configuration would look like:

    .. code-block:: yaml

       configuration:
           effect_of_risk_on_affected_risk:
               exposure_parameters: 2
               incidence_rate: 10

    """

    configuration_defaults = {
        'effect_of_risk_on_target': {
            'measure': {
                'relative_risk': None,
                'mean': None,
                'se': None,
                'log_mean': None,
                'log_se': None,
                'tau_squared': None
            }
        }
    }

    def load_relative_risk_data(self, builder):
        return get_relative_risk_data(builder, self.risk, self.target, self.randomness)

    def load_population_attributable_fraction_data(self, builder):
        return get_population_attributable_fraction_data(builder, self.risk, self.target, self.randomness)


