import itertools

from typing import NamedTuple

####################
# Project metadata #
####################

PROJECT_NAME = 'vivarium_conic_vitamin_a_supp'
CLUSTER_PROJECT = 'proj_cost_effect'

CLUSTER_QUEUE = 'all.q'
MAKE_ARTIFACT_MEM = '10G'
MAKE_ARTIFACT_CPU = '1'
MAKE_ARTIFACT_RUNTIME = '3:00:00'
MAKE_ARTIFACT_SLEEP = 10

LOCATIONS = [
    'Kenya',
    'Nigeria',
    'Burkina Faso'
]


#############
# Data Keys #
#############

METADATA_LOCATIONS = 'metadata.locations'


class __Population(NamedTuple):
    STRUCTURE: str = 'population.structure'
    AGE_BINS: str = 'population.age_bins'
    DEMOGRAPHY: str = 'population.demographic_dimensions'
    TMRLE: str = 'population.theoretical_minimum_risk_life_expectancy'
    ACMR: str = 'cause.all_causes.cause_specific_mortality_rate'
    # todo uncomment so that artifact loads this data
    # COV_LBBS_ESTIMATE: str = 'covariate.live_births_by_sex.estimate'
    COV_LBBS_ESTIMATE = 'covariate.live_births_by_sex.estimate'

    @property
    def name(self):
        return 'population'

    @property
    def log_name(self):
        return 'population'


POPULATION = __Population()


# TODO - sample key group used to identify keys in model
# For more information see the tutorial:
# https://vivarium-inputs.readthedocs.io/en/latest/tutorials/pulling_data.html#entity-measure-data
class __DIARRHEA(NamedTuple):
    DIARRHEA_CAUSE_SPECIFIC_MORTALITY_RATE: str = 'cause.diarrheal_diseases.cause_specific_mortality_rate'
    DIARRHEA_PREVALENCE: str = 'cause.diarrheal_diseases.prevalence'
    DIARRHEA_INCIDENCE_RATE: str = 'cause.diarrheal_diseases.incidence_rate'
    DIARRHEA_REMISSION_RATE: str = 'cause.diarrheal_diseases.remission_rate'
    DIARRHEA_EXCESS_MORTALITY_RATE: str = 'cause.diarrheal_diseases.excess_mortality_rate'
    DIARRHEA_DISABILITY_WEIGHT: str = 'cause.diarrheal_diseases.disability_weight'
    DIARRHEA_RESTRICTIONS: str = 'cause.diarrheal_diseases.restrictions'

    @property
    def name(self):
        return 'diarrhea'

    @property
    def log_name(self):
        return self.name.replace('_', ' ')


class __MEASLES(NamedTuple):
    MEASLES_CAUSE_SPECIFIC_MORTALITY_RATE: str = 'cause.measles.cause_specific_mortality_rate'
    MEASLES_PREVALENCE: str = 'cause.measles.prevalence'
    MEASLES_INCIDENCE_RATE: str = 'cause.measles.incidence_rate'
    MEASLES_EXCESS_MORTALITY_RATE: str = 'cause.measles.excess_mortality_rate'
    MEASLES_DISABILITY_WEIGHT: str = 'cause.measles.disability_weight'
    MEASLES_RESTRICTIONS: str = 'cause.measles.restrictions'

    @property
    def name(self):
        return 'measles'

    @property
    def log_name(self):
        return self.name.replace('_', ' ')


class __VITAMIN_A_DEFICIENCY(NamedTuple):
    VITAMIN_A_DEFICIENCY_CATEGORIES: str = 'risk_factor.vitamin_a_deficiency.categories'
    VITAMIN_A_DEFICIENCY_EXPOSURE: str = 'risk_factor.vitamin_a_deficiency.exposure'
    VITAMIN_A_DEFICIENCY_RELATIVE_RISK: str = 'risk_factor.vitamin_a_deficiency.relative_risk'
    VITAMIN_A_DEFICIENCY_PAF: str = 'risk_factor.vitamin_a_deficiency.population_attributable_fraction'
    VITAMIN_A_DEFICIENCY_DISTRIBUTION: str = 'risk_factor.vitamin_a_deficiency.distribution'
    VITAMIN_A_DEFICIENCY_RESTRICTIONS: str = 'risk_factor.vitamin_a_deficiency.restrictions'
    VITAMIN_A_DEFICIENCY_DISABILITY_WEIGHT: str = 'cause.vitamin_a_deficiency.disability_weight'

    @property
    def name(self):
        return 'vitamin_a_deficiency'

    @property
    def log_name(self):
        return self.name.replace('_', ' ')


DIARRHEA = __DIARRHEA()
MEASLES = __MEASLES()
VITAMIN_A_DEFICIENCY = __VITAMIN_A_DEFICIENCY()


MAKE_ARTIFACT_KEY_GROUPS = [
    POPULATION,
    DIARRHEA,
    MEASLES,
    VITAMIN_A_DEFICIENCY
]


###########################
# Disease Model variables #
###########################


class TransitionString(str):

    def __new__(cls, value):
        # noinspection PyArgumentList
        obj = str.__new__(cls, value.lower())
        obj.from_state, obj.to_state = value.split('_TO_')
        return obj


# TODO input details of model states and transitions
DIARRHEA_MODEL_NAME = 'diarrheal_diseases'
DIARRHEA_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{DIARRHEA_MODEL_NAME}'
DIARRHEA_WITH_CONDITION_STATE_NAME = DIARRHEA_MODEL_NAME
DIARRHEA_MODEL_STATES = (DIARRHEA_SUSCEPTIBLE_STATE_NAME, DIARRHEA_WITH_CONDITION_STATE_NAME)
DIARRHEA_MODEL_TRANSITIONS = (
    TransitionString(f'{DIARRHEA_SUSCEPTIBLE_STATE_NAME}_TO_{DIARRHEA_WITH_CONDITION_STATE_NAME}'),
    TransitionString(f'{DIARRHEA_WITH_CONDITION_STATE_NAME}_TO_{DIARRHEA_SUSCEPTIBLE_STATE_NAME}'),
)

MEASLES_MODEL_NAME = 'measles'
MEASLES_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{MEASLES_MODEL_NAME}'
MEASLES_WITH_CONDITION_STATE_NAME = MEASLES_MODEL_NAME
MEASLES_MODEL_STATES = (MEASLES_SUSCEPTIBLE_STATE_NAME, MEASLES_WITH_CONDITION_STATE_NAME)
MEASLES_MODEL_TRANSITIONS = (
    TransitionString(f'{MEASLES_SUSCEPTIBLE_STATE_NAME}_TO_{MEASLES_WITH_CONDITION_STATE_NAME}'),
    TransitionString(f'{MEASLES_WITH_CONDITION_STATE_NAME}_TO_{MEASLES_SUSCEPTIBLE_STATE_NAME}'),
)

LRI_MODEL_NAME = 'lower_respiratory_infections'
LRI_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{LRI_MODEL_NAME}'
LRI_WITH_CONDITION_STATE_NAME = LRI_MODEL_NAME
LRI_MODEL_STATES = (LRI_SUSCEPTIBLE_STATE_NAME, LRI_WITH_CONDITION_STATE_NAME)
LRI_MODEL_TRANSITIONS = (
    TransitionString(f'{LRI_SUSCEPTIBLE_STATE_NAME}_TO_{LRI_WITH_CONDITION_STATE_NAME}'),
    TransitionString(f'{LRI_WITH_CONDITION_STATE_NAME}_TO_{LRI_SUSCEPTIBLE_STATE_NAME}',)
)

VITAMIN_A_MODEL_NAME = 'vitamin_a_deficiency'
VITAMIN_A_WITH_CONDITION_STATE_NAME = VITAMIN_A_MODEL_NAME
VITAMIN_A_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{VITAMIN_A_MODEL_NAME}'
VITAMIN_A_MODEL_STATES = (VITAMIN_A_SUSCEPTIBLE_STATE_NAME, VITAMIN_A_WITH_CONDITION_STATE_NAME)
VITAMIN_A_MODEL_TRANSITIONS = (
    TransitionString(f'{VITAMIN_A_SUSCEPTIBLE_STATE_NAME}_TO_{VITAMIN_A_WITH_CONDITION_STATE_NAME}'),
    TransitionString(f'{VITAMIN_A_WITH_CONDITION_STATE_NAME}_TO_{VITAMIN_A_SUSCEPTIBLE_STATE_NAME}')
)
VITAMIN_A_BAD_EVENT_COUNT = f'{VITAMIN_A_MODEL_NAME}_event_count'
VITAMIN_A_BAD_EVENT_TIME = f'{VITAMIN_A_MODEL_NAME}_event_time'
VITAMIN_A_GOOD_EVENT_COUNT = f'{VITAMIN_A_SUSCEPTIBLE_STATE_NAME}_event_count'
VITAMIN_A_GOOD_EVENT_TIME = f'{VITAMIN_A_SUSCEPTIBLE_STATE_NAME}_event_time'
VITAMIN_A_PROPENSITY = f'{VITAMIN_A_MODEL_NAME}_propensity'
VITAMIN_A_RISK_CATEGORIES = ['cat1', 'cat2']


########################
# Risk Model Constants #
########################


#################################
# Results columns and variables #
#################################

TOTAL_POPULATION_COLUMN = 'total_population'
TOTAL_YLDS_COLUMN = 'years_lived_with_disability'
TOTAL_YLLS_COLUMN = 'years_of_life_lost'

# Columns from parallel runs
INPUT_DRAW_COLUMN = 'input_draw'
RANDOM_SEED_COLUMN = 'random_seed'
OUTPUT_SCENARIO_COLUMN = 'scenario'

STANDARD_COLUMNS = {
    'total_population': TOTAL_POPULATION_COLUMN,
    'total_ylls': TOTAL_YLLS_COLUMN,
    'total_ylds': TOTAL_YLDS_COLUMN,
}

TOTAL_POPULATION_COLUMN_TEMPLATE = 'total_population_{POP_STATE}'
PERSON_TIME_COLUMN_TEMPLATE = 'person_time_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
DEATH_COLUMN_TEMPLATE = 'death_due_to_{CAUSE_OF_DEATH}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
YLLS_COLUMN_TEMPLATE = 'ylls_due_to_{CAUSE_OF_DEATH}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
YLDS_COLUMN_TEMPLATE = 'ylds_due_to_{CAUSE_OF_DISABILITY}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
STATE_PERSON_TIME_COLUMN_TEMPLATE = '{STATE}_person_time_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
TRANSITION_COUNT_COLUMN_TEMPLATE = '{TRANSITION}_event_count_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'

COLUMN_TEMPLATES = {
    'population': TOTAL_POPULATION_COLUMN_TEMPLATE,
    'person_time': PERSON_TIME_COLUMN_TEMPLATE,
    'deaths': DEATH_COLUMN_TEMPLATE,
    'ylls': YLLS_COLUMN_TEMPLATE,
    'ylds': YLDS_COLUMN_TEMPLATE,
    'state_person_time': STATE_PERSON_TIME_COLUMN_TEMPLATE,
    'transition_count': TRANSITION_COUNT_COLUMN_TEMPLATE,
}

POP_STATES = ('living', 'dead', 'tracked', 'untracked')
SEXES = ('male', 'female')
# TODO - add literals for years in the model
YEARS = ()
# TODO - add literals for ages in the model
AGE_GROUPS = ()

CAUSES_OF_DEATH = (
    'other_causes',
    DIARRHEA_WITH_CONDITION_STATE_NAME,
    MEASLES_WITH_CONDITION_STATE_NAME,
    LRI_WITH_CONDITION_STATE_NAME
)

CAUSES_OF_DISABILITY = (
    DIARRHEA_WITH_CONDITION_STATE_NAME,
    MEASLES_WITH_CONDITION_STATE_NAME,
    LRI_WITH_CONDITION_STATE_NAME
)


DISEASE_MODELS = (DIARRHEA_MODEL_NAME, MEASLES_MODEL_NAME, LRI_MODEL_NAME, VITAMIN_A_MODEL_NAME)
DISEASE_MODEL_MAP = {
    DIARRHEA_MODEL_NAME: {
        'states': DIARRHEA_MODEL_STATES,
        'transitions': DIARRHEA_MODEL_TRANSITIONS,
    },
    MEASLES_MODEL_NAME: {
        'states': MEASLES_MODEL_STATES,
        'transitions': MEASLES_MODEL_TRANSITIONS,
    },
    LRI_MODEL_NAME: {
        'states': LRI_MODEL_STATES,
        'transitions': LRI_MODEL_TRANSITIONS,
    },
    VITAMIN_A_MODEL_NAME: {
        'states': VITAMIN_A_MODEL_STATES,
        'transitions': VITAMIN_A_MODEL_TRANSITIONS,
    }
}

STATES = (state for model in DISEASE_MODELS for state in DISEASE_MODEL_MAP[model]['states'])
TRANSITIONS = (transition for model in DISEASE_MODELS for transition in DISEASE_MODEL_MAP[model]['transitions'])

TEMPLATE_FIELD_MAP = {
    'POP_STATE': POP_STATES,
    'YEAR': YEARS,
    'SEX': SEXES,
    'AGE_GROUP': AGE_GROUPS,
    'CAUSE_OF_DEATH': CAUSES_OF_DEATH,
    'CAUSE_OF_DISABILITY': CAUSES_OF_DISABILITY,
    'STATE': STATES,
    'TRANSITION': TRANSITIONS,
}


def RESULT_COLUMNS(kind='all'):
    if kind not in COLUMN_TEMPLATES and kind != 'all':
        raise ValueError(f'Unknown result column type {kind}')
    columns = []
    if kind == 'all':
        for k in COLUMN_TEMPLATES:
            columns += RESULT_COLUMNS(k)
        columns = list(STANDARD_COLUMNS.values()) + columns
    else:
        template = COLUMN_TEMPLATES[kind]
        filtered_field_map = {field: values
                              for field, values in TEMPLATE_FIELD_MAP.items() if f'{{{field}}}' in template}
        fields, value_groups = filtered_field_map.keys(), itertools.product(*filtered_field_map.values())
        for value_group in value_groups:
            columns.append(template.format(**{field: value for field, value in zip(fields, value_group)}))
    return columns

