#!/usr/bin/env python
"""
Computes the recruitment status: consented, positive_screen, and recruited.
Exports the results to a CSV files and DB.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "ampscz-formsdb":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
import multiprocessing
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.compute.compute_recruitment_status"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

required_variables: List[str] = []


def subject_has_a_valid_baseline_form_data(
    subject_id: str, config_file: Path
) -> Tuple[bool, Optional[List[str]]]:
    """
    Check if a subject has any valid baseline form data.

    A form is considered valid if it is complete and is not marked as missing.

    Args:
        subject_id: The subject ID
        config_file: The path to the config file

    Returns:
        True if the subject has any valid baseline form data, otherwise False
        List of valid forms if any, otherwise None
    """
    recruited_status_baseline_form_requirements: List[str] = [
        "scid5_psychosis_mood_substance_abuse",
        "perceived_discrimination_scale",
        "pubertal_developmental_scale",
        "penncnb",
        "iq_assessment_wasiii_wiscv_waisiv",
        "premorbid_iq_reading_accuracy",
        "eeg_run_sheet",
        "mri_run_sheet",
        "current_health_status",
        "daily_activity_and_saliva_sample_collection",
        "blood_sample_preanalytic_quality_assurance",
        "cbc_with_differential",
        "speech_sampling_run_sheet",
        "nsipr",
        "cdss",
        "oasis",
        "assist",
        "cssrs_baseline",
        "perceived_stress_scale",
        "global_functioning_social_scale",
        "global_functioning_role_scale",
        "item_promis_for_sleep",
        "bprs",
        "pgis",
    ]

    try:
        form_status = data.get_all_form_status_for_event(
            config_file=config_file, subject_id=subject_id, event_name="baseline"
        )
    except ValueError:
        return False, None

    completed_baseline_forms: List[str] = []
    for required_form in recruited_status_baseline_form_requirements:
        is_missing = data.form_has_missing_data(
            config_file=config_file,
            subject_id=subject_id,
            form_name=required_form,
            event_name="baseline",
        )
        complete_variable = f"{required_form}_complete"
        if complete_variable in form_status:
            completion_status = form_status[complete_variable]
            if completion_status == 2:
                # If the form has a complete variable and it is set to 2,
                # assume the form is complete
                is_complete = True
            else:
                # Otherwise, assume the form is not complete
                is_complete = False
        else:
            # If the form does not have a complete variable, assume it is not complete
            is_complete = False

        if is_complete and not is_missing:
            completed_baseline_forms.append(required_form)

    completed_baseline_forms = sorted(completed_baseline_forms)

    return bool(completed_baseline_forms), completed_baseline_forms


# Define the worker function
def worker(params: Tuple[str, Path]) -> Dict[str, Any]:
    """
    Worker function to process the data for a single subject.

    Args:
        params: A tuple containing the subject ID and the config file path.
    """
    subject_id, config_file = params

    consented = data.subject_has_consent_date(
        subject_id=subject_id, config_file=config_file
    )
    included = data.subject_is_included(subject_id=subject_id, config_file=config_file)
    excluded = data.subject_is_excluded(subject_id=subject_id, config_file=config_file)
    converted = data.fetch_subject_converted(
        subject_id=subject_id, config_file=config_file
    )
    has_valid_baseline_form_data, completed_baseline_forms = (
        subject_has_a_valid_baseline_form_data(
            subject_id=subject_id, config_file=config_file
        )
    )

    # Positive screen: Has consent date + is Included
    positive_screen = consented and included

    negative_screen = consented and excluded

    # Recruited:
    #  - Converted
    #  OR
    #  - Positive screen
    #  - Atleast 1 valid baseline form data
    if converted:
        recruited = True
    else:
        recruited = positive_screen and has_valid_baseline_form_data

    subject_status = None
    if consented:
        subject_status = "consented"
    if positive_screen:
        subject_status = "positive_screen"
    if recruited:
        subject_status = "recruited"
    if negative_screen:
        subject_status = "negative_screen"

    recruitmest_status_v2 = None
    # C1 - consented
    # C2 - consented + withdrawn before baseline (withdrawal before screen outcome)
    # C3 - negative screen
    # C4 - positive screen + withdrawn before baseline (withdrawal before baseline)
    # C5 - recruited
    withdrawal_status = data.get_subject_withdrawal_status(
        config_file=config_file, subject_id=subject_id
    )

    if converted:
        recruitmest_status_v2 = "C5"
    elif withdrawal_status:
        if withdrawal_status == "not_withdrawn":
            match subject_status:
                case "consented":
                    recruitmest_status_v2 = "C1"
                case "positive_screen":
                    recruitmest_status_v2 = "C4a"
                case "recruited":
                    recruitmest_status_v2 = "C5"
                case "negative_screen":
                    recruitmest_status_v2 = "C3"
                case _:
                    recruitmest_status_v2 = None
        elif withdrawal_status == "withdrawn_before_screening_outcome":
            recruitmest_status_v2 = "C2"
        elif (
            "withdrawn_before_baseline" in withdrawal_status
            and subject_status == "positive_screen"
        ):
            recruitmest_status_v2 = "C4a"
        else:
            match subject_status:
                case "recruited":
                    recruitmest_status_v2 = "C5"
                case "consented":
                    recruitmest_status_v2 = "C1"
                case "positive_screen":
                    recruitmest_status_v2 = "C4b"
                case "negative_screen":
                    recruitmest_status_v2 = "C3"
                case _:
                    recruitmest_status_v2 = None

    recruitment_status_v2_mappping = {
        "C1": "consented",
        "C2": "withdrawn_before_screening_outcome",
        "C3": "negative_screen",
        "C4a": "withdrawn_before_baseline",
        "C4b": "positive_screen",
        "C5": "recruited",
        None: None,
    }

    recruitmest_status_v2 = recruitment_status_v2_mappping.get(recruitmest_status_v2)

    # MANUAL OVERRIDE
    if subject_id == 'LS68141':
        recruited = False
        subject_status = 'negative_screen'
        recruitmest_status_v2 = 'withdrawn_before_baseline'

    # Return a dictionary with the data for this subject
    return {
        "subject_id": subject_id,
        "consented": consented,
        "positive_screen": positive_screen,
        "negative_screen": negative_screen,
        "recruited": recruited,
        "recruitment_status": subject_status,
        "recruitment_status_v2": recruitmest_status_v2,
        "completed_baseline_forms": completed_baseline_forms,
    }


def process_subject_filters(params: Tuple[str, Path]) -> Dict[str, Any]:
    """
    Retrieve variables relevant for dashboard filters

    Args:
        params: A tuple containing the subject ID and the config file path.

    Returns:
        A dictionary containing the subject ID and the relevant variables.
    """
    subject, config_file = params

    try:
        cohort = data.get_subject_cohort(config_file=config_file, subject_id=subject)
    except ValueError:
        cohort = None
    try:
        consent_date = data.get_subject_consent_dates(
            config_file=config_file, subject_id=subject
        )
    except data.NoSubjectConsentDateException:
        consent_date = None
    chrcrit_included = data.subject_is_included(
        config_file=config_file, subject_id=subject
    )
    gender = data.get_subject_gender(config_file=config_file, subject_id=subject)
    age = data.get_subject_age(config_file=config_file, subject_id=subject)

    return {
        "subject": subject,
        "consent_date": consent_date,
        "gender": gender,
        "age": age,
        "cohort": cohort,
        "chrcrit_included": chrcrit_included,
    }


def process_data(config_file: Path) -> None:
    """
    Processes the data by fetching the data for each subject and constructing
    the SQL queries to insert the data into the database.

    Generates the recruitment status for each subject and exports the results

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
    subject_ids = data.get_all_subjects(config_file=config_file)

    subjects_count = len(subject_ids)
    logger.info(f"Exporting data for {subjects_count} subjects...")

    # Create a Pool object with the number of processes equal to the number of CPU cores / 2
    num_processes = 8
    logger.info(f"Using {num_processes} processes...")

    # Create parameters for the worker function
    worker_params = [(subject_id, config_file) for subject_id in subject_ids]
    recruitment_status_results = []
    filter_results = []

    logger.info("Computing recruitment status...")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(worker_params))
            for result in pool.imap_unordered(worker, worker_params):  # type: ignore
                recruitment_status_results.append(result)
                progress.update(task, advance=1)

    logger.info("Computing subject filters...")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(worker_params))
            for result in pool.imap_unordered(process_subject_filters, worker_params):
                filter_results.append(result)
                progress.update(task, advance=1)

    # Convert the results list into a pandas dataframe
    master_df = pd.DataFrame(recruitment_status_results)
    filters_df = pd.DataFrame(filter_results)

    logger.info("Exporting data to DB...")
    db.df_to_table(
        config_file=config_file,
        df=master_df,
        schema="forms_derived",
        table_name="recruitment_status",
        if_exists="replace",
    )

    db.df_to_table(
        config_file=config_file,
        df=filters_df,
        schema="forms_derived",
        table_name="filters",
        if_exists="replace",
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Processing data...")
    process_data(config_file=config_file)

    logger.info("Done.")
