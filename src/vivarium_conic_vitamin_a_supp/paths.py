from pathlib import Path

import vivarium_conic_vitamin_a_supp.globals as project_globals

BASE_DIR = Path(vivarium_conic_vitamin_a_supp.__file__).resolve().parent

ARTIFACT_ROOT = Path(f"/share/costeffectiveness/artifacts/{project_globals.PROJECT_NAME}/")
MODEL_SPEC_DIR = BASE_DIR / 'model_specifications'
RESULTS_ROOT = Path(f'/share/costeffectiveness/results/{project_globals.PROJECT_NAME}/')
