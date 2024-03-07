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
from typing import List, Dict, Tuple
import multiprocessing

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


def subject_has_a_valid_baseline_form_data(subject_id: str, config_file: Path) -> bool:
    """
    Check if a subject has any valid baseline form data.

    A form is considered valid if it is complete and is not marked as missing.

    Args:
        subject_id: The subject ID
        config_file: The path to the config file

    Returns:
        True if the subject has any valid baseline form data, otherwise False
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
        return False

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
            return True

    return False


def subject_completed_sociodemographics(subject_id: str, config_file: Path) -> bool:
    """
    Check if a subject has completed the sociodemographics form.

    Args:
        subject_id: The subject ID
        config_file: The path to the config file

    Returns:
        True if the subject has completed the sociodemographics form, otherwise False
    """

    form_name = "sociodemographics"
    event_name = "baseline"

    try:
        form_status = data.get_all_form_status_for_event(
            config_file=config_file, subject_id=subject_id, event_name=event_name
        )
    except ValueError:
        return False
    complete_variable = f"{form_name}_complete"

    if complete_variable in form_status:
        completion_status = form_status[complete_variable]
        if completion_status == 2:
            # Check if the form has missing data
            is_missing = data.form_has_missing_data(
                config_file=config_file,
                subject_id=subject_id,
                form_name=form_name,
                event_name=event_name,
            )
            if not is_missing:
                return True
            else:
                # Form marked complete but has missing data
                return False
        else:
            # Form not marked complete
            return False
    else:
        # Form does not have a complete variable
        return False


# Define the worker function
def worker(params: Tuple[str, Path]) -> Dict[str, str | bool | None]:
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
    has_sociodemographics = subject_completed_sociodemographics(
        subject_id=subject_id, config_file=config_file
    )
    valid_baseline_form_data = subject_has_a_valid_baseline_form_data(
        subject_id=subject_id, config_file=config_file
    )

    # Positive screen: Has consent date + is Included
    positive_screen = consented and included

    negative_screen = consented and excluded

    # Recruited:
    #  - Positive screen
    #  - Completed sociodemographics
    #  - Atleast 1 valid baseline form data
    recruited = positive_screen and has_sociodemographics and valid_baseline_form_data

    subject_status = None
    if consented:
        subject_status = "consented"
    if positive_screen:
        subject_status = "positive_screen"
    if negative_screen:
        subject_status = "negative_screen"
    if recruited:
        subject_status = "recruited"

    # Return a dictionary with the data for this subject
    return {
        "subject_id": subject_id,
        "consented": consented,
        "positive_screen": positive_screen,
        "negative_screen": negative_screen,
        "recruited": recruited,
        "recruitment_status": subject_status,
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
    num_processes = multiprocessing.cpu_count() / 2
    logger.info(f"Using {num_processes} processes...")
    pool = multiprocessing.Pool(
        processes=int(num_processes)
    )

    # Create parameters for the worker function
    worker_params = [(subject_id, config_file) for subject_id in subject_ids]

    # Use the map method to apply the worker function to each subject_id and get a list of results
    results = pool.map(worker, worker_params)

    # Close the pool and wait for the processes to finish
    pool.close()
    pool.join()

    # Convert the results list into a pandas dataframe
    master_df = pd.DataFrame(results)

    logger.info("Exporting data to DB...")
    db.df_to_table(
        config_file=config_file,
        df=master_df,
        table_name="recruitment_status",
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
