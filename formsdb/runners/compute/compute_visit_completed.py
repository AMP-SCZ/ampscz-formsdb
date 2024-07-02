#!/usr/bin/env python
"""
Infers the visit completed timepoint of subjects based on the most
recent visit with all forms completed.
(Visit Completed)
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
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.compute.compute_visit_completed"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def subject_completed_timepoint(
    config_file: Path,
    subject_id: str,
    forms_cohort_timepoint_map: Dict[str, Dict[str, List[str]]],
    timepoint: str,
) -> Tuple[bool, Dict[str, bool]]:
    """
    Returns the timepoint of the most recent visit form completed by the subject.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): The subject ID.
        forms_cohort_timepoint_map (Dict[str, Dict[str, List[str]]]): A dictionary
            containing the forms for each timepoint.
        timepoint (str): The timepoint to check if the subject has started.

    Returns:
        Tuple[bool, Dict[str, bool]]: A tuple containing a boolean indicating if the
            subject has completed the timepoint and a dictionary containing the
            completion status of each form.
    """
    try:
        cohort = data.get_subject_cohort(config_file=config_file, subject_id=subject_id)
    except ValueError:
        # Assume that the subject is CHR
        cohort = "CHR"

    if cohort is None:
        # Assume that the subject is CHR
        cohort = "CHR"

    subject_forms = forms_cohort_timepoint_map[cohort.lower()]
    digital_biomarkers_forms = [
        "digital_biomarkers_mindlamp_onboarding",
        "digital_biomarkers_axivity_onboarding",
        "digital_biomarkers_mindlamp_checkin",
        "digital_biomarkers_axivity_checkin",
        "digital_biomarkers_mindlamp_end_of_12month__study_p",
        "digital_biomarkers_axivity_end_of_12month__study_pe",
    ]
    skipped_forms = [
        "coenrollment_form",
        "sociodemographics",
    ] + digital_biomarkers_forms

    # does not have a completed variable
    skipped_forms.append("family_interview_for_genetic_studies_figs")

    forms = subject_forms[timepoint]

    if data.subject_uses_rpms(config_file=config_file, subject_id=subject_id):
        if "psychs_p1p8_fu_hc" in forms:
            forms.remove("psychs_p1p8_fu_hc")
            forms.append("psychs_p1p8_fu")

        if "psychs_p9ac32_fu_hc" in forms:
            forms.remove("psychs_p9ac32_fu_hc")
            forms.append("psychs_p9ac32_fu")

    forms_completed_map = {}
    completed = True

    try:
        for form in forms:
            if form in skipped_forms:
                continue
            is_complete = data.form_is_complete(
                config_file=config_file,
                subject_id=subject_id,
                form_name=form,
                event_name=timepoint,
            )

            if not is_complete:
                completed = False

            is_missing = False

            if is_complete and not is_missing:
                pass
            else:
                completed = False

            forms_completed_map[form] = is_complete and not is_missing
    except ValueError:
        return False, forms_completed_map

    if completed:
        missing: bool = True
        # If all forms are missing, then the timepoint is not completed
        for form in forms:
            if form in skipped_forms:
                continue
            is_missing = data.form_has_missing_data(
                config_file=config_file,
                subject_id=subject_id,
                form_name=form,
                event_name=timepoint,
            )
            if not is_missing:
                missing = False
                # If any form is not missing, then
                # the timepoint is completed
                break

        if missing:
            completed = False

    return completed, forms_completed_map


def process_subject(params: Tuple[Path, str]) -> List[Dict[str, Any]]:
    """
    Process the subject and return the data.

    Args:
        params (Tuple[Path, str]): A tuple containing the config file and the subject ID.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the data for the subject.
    """
    config_file, subject_id = params
    forms_cohort_timepoint_map = data.get_forms_cohort_timepoint_map(
        config_file=config_file
    )
    timepoints = constants.visit_order

    results = []
    for timepoint in timepoints:
        is_timepoint_completed, all_form_map = subject_completed_timepoint(
            config_file=config_file,
            subject_id=subject_id,
            forms_cohort_timepoint_map=forms_cohort_timepoint_map,
            timepoint=timepoint,
        )

        # Remove keys with True values
        not_completed_form_map = {k: v for k, v in all_form_map.items() if not v}
        not_completed_forms = list(not_completed_form_map.keys())
        completed_forms = [
            form for form in all_form_map.keys() if form not in not_completed_forms
        ]

        result = {
            "subject_id": subject_id,
            "timepoint": timepoint,
            "completed": is_timepoint_completed,
            # "forms_completed_map": all_form_map,
            "completed_forms": completed_forms,
            "not_completed_forms": not_completed_forms,
        }
        results.append(result)

    return results


def compute_visit_completed_data(config_file: Path) -> pd.DataFrame:
    """
    Aggregates all completed and not completed forms for each subject.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        pd.DataFrame: The visit completed data.
    """
    subject_ids = data.get_all_subjects(config_file=config_file)
    params = [(config_file, subject) for subject in subject_ids]
    results = []

    num_processes = 8
    logger.info(f"Using {num_processes} processes")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for result in pool.imap_unordered(process_subject, params):  # type: ignore
                results.extend(result)
                progress.update(task, advance=1)

    completed_forms_df = pd.DataFrame(results)
    return completed_forms_df


def get_subject_visit_completed_data(
    config_file: Path,
    subject_id: str,
) -> pd.DataFrame:
    """
    Get the subject visit completed data.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): The subject ID.
        timepoint (Optional[str], optional): The timepoint to filter by. Defaults to None.

    Returns:
        pd.DataFrame: The subject visit completed data.
    """
    query = (
        f"SELECT * FROM subject_visit_completed_data WHERE subject_id = '{subject_id}'"
    )

    completed_forms_df = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    return completed_forms_df


def get_subject_visit_completed(
    config_file: Path,
    subject_id: str,
) -> Optional[str]:
    """
    Returns the furthest timepoint the subject has completed, consecutively.

    A completed visit is defined as having all forms completed.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): The subject ID.

    Returns:
        Optional[str]: The furthest timepoint the subject has completed, consecutively.
    """

    subject_data = get_subject_visit_completed_data(
        config_file=config_file,
        subject_id=subject_id,
    )

    if subject_data is None:
        return None

    timepoints = constants.visit_order
    completed_timepoint = None

    for timepoint in timepoints:
        timepoint_data = subject_data[subject_data["timepoint"] == timepoint]
        timepoint_completed = timepoint_data["completed"].values[0]

        # if not timepoint_completed:
        #     prev_timepoint_idx = timepoints.index(timepoint)
        #     if prev_timepoint_idx == 0:
        #         return None
        #     prev_timepoint = timepoints[prev_timepoint_idx - 1]
        #     return prev_timepoint

        # return timepoints[-1]

        if timepoint_completed:
            completed_timepoint = timepoint

    return completed_timepoint


def process_subject_visit_completed(params: Tuple[Path, str]) -> Dict[str, Any]:
    """
    Process the subject and return the data.

    Args:
        params (Tuple[Path, str]): A tuple containing the config file and the subject ID.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the data for the subject.
    """
    config_file, subject_id = params

    furthest_timepoint = get_subject_visit_completed(
        config_file=config_file,
        subject_id=subject_id,
    )

    result = {
        "subject_id": subject_id,
        "completed_timepoint": furthest_timepoint,
    }

    return result


def compute_visit_completed(config_file: Path) -> pd.DataFrame:
    """
    Compute the visit completed timepoint for all subjects.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        pd.DataFrame: The visit completed data, with subject ID and the furthest
            timepoint completed.
    """
    subject_ids = data.get_all_subjects(config_file=config_file)
    params = [(config_file, subject) for subject in subject_ids]
    completed_results: List[Dict[str, Any]] = []

    num_processes = 8
    logger.info(f"Using {num_processes} processes")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for result in pool.imap_unordered(process_subject_visit_completed, params):  # type: ignore
                completed_results.append(result)
                progress.update(task, advance=1)

    complete_df = pd.DataFrame(completed_results)
    return complete_df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    logger.info("Aggregating all forms comleted for each subject...")
    visit_completed_df = compute_visit_completed_data(config_file=config_file)
    db.df_to_table(
        df=visit_completed_df,
        table_name="subject_visit_completed_data",
        config_file=config_file,
        if_exists="replace",
    )

    logger.info("Computing the furthest timepoint completed for each subject...")
    visit_completed_df = compute_visit_completed(config_file=config_file)
    db.df_to_table(
        df=visit_completed_df,
        table_name="subject_visit_completed",
        config_file=config_file,
        if_exists="replace",
    )

    logger.info("Done.")
