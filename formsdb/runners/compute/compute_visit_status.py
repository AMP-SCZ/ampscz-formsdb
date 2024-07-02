#!/usr/bin/env python
"""
Infers the visit status of subjects based on the most recent visit form completed.
(Visit Started)
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
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.compute.compute_visit_status"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_subject_started_visit(
    config_file: Path,
    subject_id: str,
    forms_cohort_timepoint_map: Dict[str, Dict[str, List[str]]],
) -> str:
    """
    Returns the timepoint of the most recent visit form completed by the subject.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): The subject ID.
        forms_cohort_timepoint_map (Dict[str, Dict[str, List[str]]]): A dictionary
            containing the forms for each timepoint.

    Returns:
        str: The timepoint of the most recent visit form completed by the subject.
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
    subject_timepoints = list(subject_forms.keys())

    current_timepoint = subject_timepoints[0]
    for timepoint in subject_timepoints:
        forms = subject_forms[timepoint]
        timepoint_not_found = False
        try:
            for form in forms:
                is_complete = data.form_is_complete(
                    config_file=config_file,
                    subject_id=subject_id,
                    form_name=form,
                    event_name=timepoint,
                )

                if not is_complete:
                    continue

                is_missing = data.form_has_missing_data(
                    config_file=config_file,
                    subject_id=subject_id,
                    form_name=form,
                    event_name=timepoint,
                )
                if is_complete and not is_missing:
                    if "digital_biomarkers" in form or "sociodemographics" in form:
                        # Skip: not a reliable form for determining timepoint
                        continue
                    current_timepoint = timepoint
                    # print(f"Timepoint: {timepoint}, Form: {form} is complete")
                    break
        except ValueError:
            # print(f"Subject: {subject_id}, Timepoint: {timepoint} does not exist")
            timepoint_not_found = True
            continue

        timepoint_delta = subject_timepoints.index(
            timepoint
        ) - subject_timepoints.index(current_timepoint)
        if (
            timepoint_delta > 6
            and not timepoint_not_found
            and cohort.lower()
            == "chr"  # Only for CHR subjects; HC subjects can have large gaps
        ):
            # print(f"Timepoint: {timepoint} has not started")
            break

    # print(f"Current timepoint: {current_timepoint}")
    return current_timepoint


def process_subject(
    params: Tuple[Path, str, Dict[str, Dict[str, List[str]]]]
) -> Dict[str, Optional[str]]:
    """
    Wrapper function for the `get_subject_started_visit` function.

    Args:
        params (Tuple[Path, str, Dict[str, Dict[str, List[str]]]]): A tuple containing the
            config file, subject ID, and the forms_cohort_timepoint_map.

    Returns:
        Dict[str, Union[str, datetime, Dict[str, Dict[str, List[str]]]]]: A dictionary
            containing the subject ID and the timepoint of the most recent visit form
            completed by the subject.
    """
    config_file, subject, forms_cohort_timepoint_map = params
    timepoint = get_subject_started_visit(
        config_file=config_file,
        subject_id=subject,
        forms_cohort_timepoint_map=forms_cohort_timepoint_map,
    )
    return {"subject_id": subject, "timepoint": timepoint}


def compute_recent_visit(config_file: Path) -> pd.DataFrame:
    """
    Compute the most recent visit of each subject.

    Args:
        config_file (Path): Path to the config file.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame containing the most recent visit of each subject.
    """
    logger.info("Computing each subject's most recent visit...")

    subjects = data.get_all_subjects(config_file=config_file)
    subjects_count = len(subjects)
    logger.info(f"Found {subjects_count} subjects.")

    forms_cohort_timepoint_map = data.get_forms_cohort_timepoint_map(
        config_file=config_file
    )

    num_processes = 8
    logger.info(f"Using {num_processes} processes.")

    params = [
        (config_file, subject, forms_cohort_timepoint_map) for subject in subjects
    ]
    results = []

    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for result in pool.imap_unordered(process_subject, params):  # type: ignore
                results.append(result)
                progress.update(task, advance=1)

    df = pd.DataFrame(results)

    return df


# superset cumulative charts (Visit Started)
def subject_started_timepoint_fetch(
    config_file: Path,
    subject_id: str,
    timepoint: str,
) -> bool:
    """
    Checks if the subject has started the given timepoint.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): The subject ID.
        timepoint (str): The timepoint to check.

    Returns:
        bool: True if the subject has started the timepoint, False otherwise.
    """
    visit_order = constants.visit_order
    timepoint_index = visit_order.index(timepoint)

    current_visit = data.get_subject_latest_visit_started(
        config_file=config_file, subject_id=subject_id
    )
    if current_visit is None:
        return False
    current_visit_index = visit_order.index(current_visit)

    if current_visit_index >= timepoint_index:
        return True
    return False


def process_subject_fetch(params: Tuple) -> Dict[str, Any]:
    """
    Wrapper function for the `subject_started_timepoint_fetch` function.

    Args:
        params (Tuple): A tuple containing the config file, subject ID, and timepoints.

    Returns:
        Dict[str, Any]: A dictionary containing the subject ID if each timepoint has started.
    """
    config_file, subject_id, timepoints = params

    result = {}
    result["subject_id"] = subject_id
    for timepoint in timepoints:
        subject_started = subject_started_timepoint_fetch(
            config_file=config_file,
            subject_id=subject_id,
            timepoint=timepoint,
        )
        result[timepoint] = subject_started

    return result


def populate_cumulative_charts_data(config_file: Path) -> None:
    """
    Compute the cumulative data for each subject. Used for the Superset dashboard.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        None
    """
    subject_ids = data.get_all_subjects(config_file=config_file)
    timepoints = ["baseline", "month_1", "month_2", "month_6", "month_12", "month_18"]

    params = [(config_file, subject_id, timepoints) for subject_id in subject_ids]
    num_processes = 8
    results = []

    logger.info(f"Using {num_processes} processes.")
    logger.info("Computing cumulative data...")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for result in pool.imap_unordered(process_subject_fetch, params):  # type: ignore
                results.append(result)
                progress.update(task, advance=1)

    results_df = pd.DataFrame(results)
    reversed_timepoints = timepoints.copy()
    reversed_timepoints.reverse()

    overridden_df = results_df.copy()

    # Override the values, so if a more recent visit has started,
    # the previous visits are also marked as started
    for idx, row in overridden_df.iterrows():
        override = False
        for timepoint in reversed_timepoints:
            if row[timepoint]:
                override = True

            if override:
                overridden_df.loc[idx, timepoint] = True  # type: ignore

    logger.info("Writing cumulative data to the database...")
    db.df_to_table(
        df=results_df,
        table_name="subject_started_timepoint",
        config_file=config_file,
        if_exists="replace",
    )

    db.df_to_table(
        df=overridden_df,
        table_name="subject_started_timepoint_cumulative",
        config_file=config_file,
        if_exists="replace",
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    visit_status_df = compute_recent_visit(config_file=config_file)

    logger.info("Saving to the database...")
    db.df_to_table(
        df=visit_status_df,
        table_name="subject_visit_status",
        config_file=config_file,
        if_exists="replace",
    )

    logger.info("Populating cumulative charts data...")
    populate_cumulative_charts_data(config_file=config_file)

    logger.info("Done!")
