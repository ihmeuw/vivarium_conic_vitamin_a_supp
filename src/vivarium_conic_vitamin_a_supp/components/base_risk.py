"""
===================
Risk Exposure Model
===================

This module contains tools for modeling categorical and continuous risk
exposure.

"""
import pandas as pd

from vivarium_public_health.utilities import EntityString
from .distributions import SimulationDistribution
from vivarium_public_health.risks.base_risk import Risk as Risk_


class Risk(Risk_):
    """
    Work around naming incompatibility problem by using local SimulationDistribution
    """

    configuration_defaults = {
        "risk": {
            "exposure": 'data',
            "rebinned_exposed": [],
            "category_thresholds": [],
        }
    }

    def __init__(self, risk: str):
        """
        Parameters
        ----------
        risk :
            the type and name of a risk, specified as "type.name". Type is singular.
        """
        self.risk = EntityString(risk)
        self.configuration_defaults = {f'{self.risk.name}': Risk.configuration_defaults['risk']}
        self.exposure_distribution = SimulationDistribution(self.risk)
        self._sub_components = [self.exposure_distribution]
