"""Modularized functions for building project data artifacts.

This module is an abstraction around the load portion of our artifact building ETL pipeline.
The intent is to be declarative so it's easy to see what is put into the artifact and how.
Some degree of verbosity/boilerplate is fine in the interest of transparancy.

.. admonition::

   Logging in this module should be done at the ``debug`` level.

"""
from pathlib import Path

from loguru import logger
import pandas as pd
from vivarium.framework.artifact import Artifact, get_location_term, EntityKey

from vivarium_conic_vitamin_a_supp import globals as project_globals
from vivarium_conic_vitamin_a_supp.data import loader


def open_artifact(output_path: Path, location: str) -> Artifact:
    """Creates or opens an artifact at the output path.

    Parameters
    ----------
    output_path
        Fully resolved path to the artifact file.
    location
        Proper GBD location name represented by the artifact.

    Returns
    -------
        A new artifact.

    """
    if not output_path.exists():
        logger.debug(f"Creating artifact at {str(output_path)}.")
    else:
        logger.debug(f"Opening artifact at {str(output_path)} for appending.")

    artifact = Artifact(output_path, filter_terms=[get_location_term(location)])

    key = project_globals.METADATA_LOCATIONS
    if key not in artifact:
        artifact.write(key, [location])

    return artifact


def load_and_write_data(artifact: Artifact, key: str, location: str):
    """Loads data and writes it to the artifact if not already present.

    Parameters
    ----------
    artifact
        The artifact to write to.
    key
        The entity key associated with the data to write.
    location
        The location associated with the data to load and the artifact to
        write to.

    """
    if key in artifact:
        logger.debug(f'Data for {key} already in artifact.  Skipping...')
    else:
        logger.debug(f'Loading data for {key} for location {location}.')
        data = loader.get_data(key, location)
        logger.debug(f'Writing data for {key} to artifact.')
        artifact.write(key, data)
    return artifact.load(key)


def write_data(artifact: Artifact, key: str, data: pd.DataFrame):
    """Writes data to the artifact if not already present.

    Parameters
    ----------
    artifact
        The artifact to write to.
    key
        The entity key associated with the data to write.
    data
        The data to write.

    """
    if key in artifact:
        logger.debug(f'Data for {key} already in artifact.  Skipping...')
    else:
        logger.debug(f'Writing data for {key} to artifact.')
        artifact.write(key, data)
    return artifact.load(key)

def load_and_write_demographic_data(artifact: Artifact, location: str):
    keys = [
        project_globals.POPULATION.POPULATION_STRUCTURE,
        project_globals.POPULATION.POPULATION_AGE_BINS,
        project_globals.POPULATION.POPULATION_DEMOGRAPHY,
        project_globals.POPULATION.POPULATION_TMRLE,  # Theoretical life expectancy
        project_globals.POPULATION.ALL_CAUSE_CSMR,
        project_globals.POPULATION.COVARIATE_LIVE_BIRTHS_BY_SEX,
    ]

    for key in keys:
        load_and_write_data(artifact, key, location)


def load_and_write_diarrhea_data(artifact: Artifact, location: str):
    keys = [
        project_globals.DIARRHEA.DIARRHEA_PREVALENCE,
        project_globals.DIARRHEA.DIARRHEA_INCIDENCE_RATE,
        project_globals.DIARRHEA.DIARRHEA_REMISSION_RATE,
        project_globals.DIARRHEA.DIARRHEA_CAUSE_SPECIFIC_MORTALITY_RATE,
        project_globals.DIARRHEA.DIARRHEA_EXCESS_MORTALITY_RATE,
        project_globals.DIARRHEA.DIARRHEA_DISABILITY_WEIGHT,
        project_globals.DIARRHEA.DIARRHEA_RESTRICTIONS
    ]

    for key in keys:
        load_and_write_data(artifact, key, location)


def load_and_write_measles_data(artifact: Artifact, location: str):
    keys = [
        project_globals.MEASLES.MEASLES_PREVALENCE,
        project_globals.MEASLES.MEASLES_INCIDENCE_RATE,
        project_globals.MEASLES.MEASLES_CAUSE_SPECIFIC_MORTALITY_RATE,
        project_globals.MEASLES.MEASLES_EXCESS_MORTALITY_RATE,
        project_globals.MEASLES.MEASLES_DISABILITY_WEIGHT,
        project_globals.MEASLES.MEASLES_RESTRICTIONS
    ]

    for key in keys:
        load_and_write_data(artifact, key, location)


def load_and_write_lri_data(artifact: Artifact, location: str):
    keys = [
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_PREVALENCE,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_BIRTH_PREVALENCE,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_INCIDENCE_RATE,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_REMISSION_RATE,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_CAUSE_SPECIFIC_MORTALITY_RATE,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_EXCESS_MORTALITY_RATE,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_DISABILITY_WEIGHT,
        project_globals.LOWER_RESPIRATORY_INFECTIONS.LRI_RESTRICTIONS,
    ]

    for key in keys:
        load_and_write_data(artifact, key, location)


def load_and_write_vitamin_a_deficiency_data(artifact: Artifact, location: str):
    from vivarium_conic_vitamin_a_supp.globals import VITAMIN_A_DEFICIENCY as vad
    keys = [
        vad.VITAMIN_A_DEFICIENCY_CATEGORIES,
        vad.VITAMIN_A_DEFICIENCY_RESTRICTIONS,
        vad.VITAMIN_A_DEFICIENCY_EXPOSURE,
        vad.VITAMIN_A_DEFICIENCY_RELATIVE_RISK,
        vad.VITAMIN_A_DEFICIENCY_PAF,
        vad.VITAMIN_A_DEFICIENCY_DISTRIBUTION,
        vad.VITAMIN_A_DEFICIENCY_DISABILITY_WEIGHT,
    ]

    for key in keys:
        load_and_write_data(artifact, key, location)
