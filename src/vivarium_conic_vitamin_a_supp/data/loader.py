"""Loads, standardizes and validates input data for the simulation.

Abstract the extract and transform pieces of the artifact ETL.
The intent here is to provide a uniform interface around this portion
of artifact creation. The value of this interface shows up when more
complicated data needs are part of the project. See the BEP project
for an example.

`BEP <https://github.com/ihmeuw/vivarium_gates_bep/blob/master/src/vivarium_gates_bep/data/loader.py>`_

.. admonition::

   No logging is done here. Logging is done in vivarium inputs itself and forwarded.
"""
from gbd_mapping import causes, risk_factors, covariates
import pandas as pd
from vivarium.framework.artifact import EntityKey
from vivarium_inputs import interface, utilities, utility_data, globals as vi_globals
from vivarium_inputs.mapping_extension import alternative_risk_factors
from get_draws.api import get_draws

from vivarium_conic_vitamin_a_supp import globals as project_globals


def get_data(lookup_key: str, location: str) -> pd.DataFrame:
    """Retrieves data from an appropriate source.

    Parameters
    ----------
    lookup_key
        The key that will eventually get put in the artifact with
        the requested data.
    location
        The location to get data for.

    Returns
    -------
        The requested data.

    """
    mapping = {
        project_globals.POPULATION.STRUCTURE: load_population_structure,
        project_globals.POPULATION.AGE_BINS: load_age_bins,
        project_globals.POPULATION.DEMOGRAPHY: load_demographic_dimensions,
        project_globals.POPULATION.TMRLE: load_theoretical_minimum_risk_life_expectancy,
        project_globals.POPULATION.ACMR: load_standard_data,
        project_globals.POPULATION.COV_LBBS_ESTIMATE: load_standard_data,

        project_globals.DIARRHEA.DIARRHEA_PREVALENCE: load_standard_data,
        project_globals.DIARRHEA.DIARRHEA_INCIDENCE_RATE: load_standard_data,
        project_globals.DIARRHEA.DIARRHEA_REMISSION_RATE: load_standard_data,
        project_globals.DIARRHEA.DIARRHEA_CAUSE_SPECIFIC_MORTALITY_RATE: load_standard_data,
        project_globals.DIARRHEA.DIARRHEA_EXCESS_MORTALITY_RATE: load_standard_data,
        project_globals.DIARRHEA.DIARRHEA_DISABILITY_WEIGHT: load_standard_data,
        project_globals.DIARRHEA.DIARRHEA_RESTRICTIONS: load_metadata,
        
        project_globals.MEASLES.MEASLES_PREVALENCE: load_standard_data,
        project_globals.MEASLES.MEASLES_INCIDENCE_RATE: load_standard_data,
        project_globals.MEASLES.MEASLES_CAUSE_SPECIFIC_MORTALITY_RATE: load_standard_data,
        project_globals.MEASLES.MEASLES_EXCESS_MORTALITY_RATE: load_standard_data,
        project_globals.MEASLES.MEASLES_DISABILITY_WEIGHT: load_standard_data,
        project_globals.MEASLES.MEASLES_RESTRICTIONS: load_metadata,

        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_PREVALENCE: load_standard_data,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_BIRTH_PREVALENCE: load_lri_birth_prevalence_from_meid,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_INCIDENCE_RATE: load_standard_data,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_REMISSION_RATE: load_standard_data,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_CAUSE_SPECIFIC_MORTALITY_RATE: load_standard_data,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_EXCESS_MORTALITY_RATE: load_standard_data,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_DISABILITY_WEIGHT: load_standard_data,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_RESTRICTIONS: load_metadata,


        project_globals.VITAMIN_A_DEFICIENCY.VITAMIN_A_DEFICIENCY_CATEGORIES: load_metadata,
        project_globals.VITAMIN_A_DEFICIENCY.VITAMIN_A_DEFICIENCY_RESTRICTIONS: load_metadata,
        project_globals.VITAMIN_A_DEFICIENCY.VITAMIN_A_DEFICIENCY_DISABILITY_WEIGHT: load_standard_data,
        project_globals.VITAMIN_A_DEFICIENCY.VITAMIN_A_DEFICIENCY_EXPOSURE: load_standard_data,
        project_globals.VITAMIN_A_DEFICIENCY.VITAMIN_A_DEFICIENCY_RELATIVE_RISK: load_standard_data,
        project_globals.VITAMIN_A_DEFICIENCY.VITAMIN_A_DEFICIENCY_PAF: load_standard_data,
        project_globals.VITAMIN_A_DEFICIENCY.VITAMIN_A_DEFICIENCY_DISTRIBUTION: load_metadata,
    }
    return mapping[lookup_key](lookup_key, location)


def load_population_structure(key: str, location: str) -> pd.DataFrame:
    return interface.get_population_structure(location)


def load_age_bins(key: str, location: str) -> pd.DataFrame:
    return interface.get_age_bins()


def load_demographic_dimensions(key: str, location: str) -> pd.DataFrame:
    return interface.get_demographic_dimensions(location)


def load_theoretical_minimum_risk_life_expectancy(key: str, location: str) -> pd.DataFrame:
    return interface.get_theoretical_minimum_risk_life_expectancy()


def load_standard_data(key: str, location: str) -> pd.DataFrame:
    key = EntityKey(key)
    entity = get_entity(key)
    return interface.get_measure(entity, key.measure, location)


def load_metadata(key: str, location: str):
    key = EntityKey(key)
    entity = get_entity(key)
    metadata = entity[key.measure]
    if hasattr(metadata, 'to_dict'):
        metadata = metadata.to_dict()
    return metadata


def load_lri_birth_prevalence_from_meid(_, location):
    """Ignore the first argument to fit in to the get_data model. """
    location_id = utility_data.get_location_id(location)
    data = get_draws('modelable_entity_id', project_globals.LRI_BIRTH_PREVALENCE_MEID,
                     source=project_globals.LRI_BIRTH_PREVALENCE_DRAW_SOURCE,
                     age_group_id=project_globals.LRI_BIRTH_PREVALENCE_AGE_ID,
                     measure_id=vi_globals.MEASURES['Prevalence'],
                     gbd_round_id=project_globals.LRI_BIRTH_PREVALENCE_GBD_ROUND,
                     location_id=location_id)
    data = data[data.measure_id == vi_globals.MEASURES['Prevalence']]
    data = utilities.normalize(data, fill_value=0)

    idx_columns = list(vi_globals.DEMOGRAPHIC_COLUMNS)
    idx_columns.remove('age_group_id')
    data = data.filter(idx_columns + vi_globals.DRAW_COLUMNS)

    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)



# TODO - add project-specific data functions here


def get_entity(key: str):
    # Map of entity types to their gbd mappings.
    type_map = {
        'cause': causes,
        'covariate': covariates,
        'risk_factor': risk_factors,
        'alternative_risk_factor': alternative_risk_factors
    }
    key = EntityKey(key)
    return type_map[key.type][key.name]
