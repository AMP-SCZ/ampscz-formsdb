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
from typing import Dict, List, Optional

import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
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


def subject_has_consent_date(subject_id: str, config_file: Path) -> bool:
    """
    Check if a subject has a consent date

    Args:
        subject_id: The subject ID
        config_file: The path to the config file

    Returns:
        True if the subject has a consent date, otherwise False
    """

    try:
        data.get_subject_consent_dates(config_file=config_file, subject_id=subject_id)
        return True
    except data.NoSubjectConsentDateException:
        return False


def subject_is_included(subject_id: str, config_file: Path) -> bool:
    """
    Check if a subject is included in the study.

    Based on 'chrcrit_included' variable from '

    Args:
        subject_id: The subject ID
        config_file: The path to the config file

    Returns:
        True if the subject is included, otherwise False
    """

    form_df = data.get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    # Get 'inclusionexclusion_criteria_review' form
    form_id = "inclusionexclusion_criteria_review"

    icr_df = form_df[form_df["form_name"] == form_id]
    icr_df.reset_index(drop=True, inplace=True)
    icr_df = utils.explode_col(df=icr_df, col="form_data")

    # Check if 'chrcrit_included' is 1
    try:
        included = icr_df["chrcrit_included"].iloc[0]
    except KeyError:
        included = False

    if included == 1:
        return True
    else:
        return False


def subject_has_missing_data(
    config_file: Path,
    subject_id: str,
    form_name: str,
    event_name: Optional[str] = None,
) -> Optional[bool]:
    """
    Check if a subject has missing data for a given form.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID
        form_name: The form name

    Returns:
        Optional[bool]: True if the subject has missing data, otherwise False.
            None if form does not exist for the subject.
    """

    form_df = data.get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    form_df = form_df[form_df["form_name"] == form_name]
    if event_name:
        form_df = form_df[form_df["event_name"].str.contains(event_name)]
    form_df.reset_index(drop=True, inplace=True)

    if form_df.empty:
        return None

    form_df = utils.explode_col(df=form_df, col="form_data")

    form_abbrv = constants.form_name_to_abbrv[form_name]
    missing_variable = f"{form_abbrv}_missing"

    try:
        missing = form_df[missing_variable].iloc[0]
    except KeyError:
        return False

    if missing == 1:
        return True
    else:
        return False


def get_form_status(
    config_file: Path,
    subject_id: str,
    event_name: str,
) -> Dict[str, int]:
    """
    Get the form status for a subject's REDCap event.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID
        event_name: The REDCap event name

    Returns:
        Dict[str, int]: A dictionary containing the form status for the event.
            Legend:
            - 0: Not started
            - 1: In progress
            - 2: Completed
    """

    form_df = data.get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    # Filter by event name and form_name = 'uncategorized'
    form_df = form_df[form_df["event_name"].str.contains(event_name)]
    form_df = form_df[form_df["form_name"] == "uncategorized"]

    # reset index
    form_df.reset_index(drop=True, inplace=True)

    if form_df.empty:
        raise ValueError(
            f"No 'uncategorized' form for subject {subject_id} and event {event_name}"
        )

    # Read JSON from form_data column as Dict
    form_data: Dict[str, int] = form_df["form_data"].iloc[0]

    return form_data


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
        form_status = get_form_status(
            config_file=config_file, subject_id=subject_id, event_name="baseline"
        )
    except ValueError:
        return False

    for required_form in recruited_status_baseline_form_requirements:
        is_missing = subject_has_missing_data(
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
        form_status = get_form_status(
            config_file=config_file, subject_id=subject_id, event_name=event_name
        )
    except ValueError:
        return False
    complete_variable = f"{form_name}_complete"

    if complete_variable in form_status:
        completion_status = form_status[complete_variable]
        if completion_status == 2:
            # Check if the form has missing data
            is_missing = subject_has_missing_data(
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

    with utils.get_progress_bar() as progress:
        task = progress.add_task("Processing...", total=subjects_count)
        master_df = pd.DataFrame()

        for subject_id in subject_ids:
            progress.update(task, advance=1, description=f"Processing {subject_id}...")

            consented = subject_has_consent_date(
                subject_id=subject_id, config_file=config_file
            )
            included = subject_is_included(
                subject_id=subject_id, config_file=config_file
            )
            has_sociodemographics = subject_completed_sociodemographics(
                subject_id=subject_id, config_file=config_file
            )
            valid_baseline_form_data = subject_has_a_valid_baseline_form_data(
                subject_id=subject_id, config_file=config_file
            )

            # Positive screen: Has consent date + is Included
            positive_screen = consented and included

            # Recruited:
            #  - Positive screen
            #  - Completed sociodemographics
            #  - Atleast 1 valid baseline form data
            recruited = (
                positive_screen and has_sociodemographics and valid_baseline_form_data
            )

            subject_status = None
            if consented:
                subject_status = "consented"
            if positive_screen:
                subject_status = "positive_screen"
            if recruited:
                subject_status = "recruited"

            # Add to master DataFrame
            master_df = pd.concat(
                [
                    master_df,
                    pd.DataFrame(
                        {
                            "subject_id": [subject_id],
                            "consented": [consented],
                            "positive_screen": [positive_screen],
                            "recruited": [recruited],
                            "recruitment_status": [subject_status],
                        }
                    ),
                ]
            )

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
