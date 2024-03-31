#!/usr/bin/env python
"""
Updates the subject_removed table in the database with the removed status of subjects.
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
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.compute.compute_removed"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def count_removed(config_file: Path) -> int:
    """
    Count the number of subjects that have been removed.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        int: Number of subjects that have been removed.
    """
    query = "SELECT COUNT(*) FROM subject_removed WHERE removed = 'True';"
    removed_count_r = db.fetch_record(config_file=config_file, query=query)
    if removed_count_r is None:
        raise ValueError("No removed subjects found in the database.")
    removed_count = int(removed_count_r)

    return removed_count


def check_if_removed(
    subject_id: str, visit_order: List[str], config_file: Path, debug: bool = False
) -> Optional[Tuple[str, str, Optional[datetime]]]:
    """
    Check if a subject has been removed.

    Args:
        subject_id (str): Subject ID.
        config_file (Path): Path to the config file.
        visit_order (List[str]): List of visits in the order they were conducted.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        Optional[Tuple[str, str]]: If the subject was removed, returns the event and reason.
    """
    df = data.get_all_subject_forms(subject_id=subject_id, config_file=config_file)
    missing_df = df[df["form_name"].str.contains("missing_data")]

    if missing_df.shape[0] == 0:
        return None

    for visit in visit_order:
        visit_psychs_df = missing_df[missing_df["event_name"].str.contains(f"{visit}_")]
        if visit_psychs_df.shape[0] == 0:
            if debug:
                print(f"No missing_data form found for {visit}.")
            continue

        form_r = visit_psychs_df.iloc[0, :]
        form_data: Dict[str, Any] = form_r["form_data"]

        form_variables = list(form_data.keys())

        withdrawn_variable = "chrmiss_withdrawn"
        if withdrawn_variable in form_variables:
            if form_data[withdrawn_variable] == 1:
                withdrawn_date = data.estimate_event_date(
                    subject_id=subject_id, event=visit, config_file=config_file
                )
                if debug:
                    print(f"Subject withdrew at {visit} on {withdrawn_date}.")
                return visit, "withdrawn", withdrawn_date
        else:
            if debug:
                print(f"{withdrawn_variable} not found in {visit}.")

        discontinued_variable = "chrmiss_discon"
        if discontinued_variable in form_variables:
            if form_data[discontinued_variable] == 1:
                withdrawn_date = data.estimate_event_date(
                    subject_id=subject_id, event=visit, config_file=config_file
                )
                if debug:
                    print(f"Subject discontinued at {visit} on {withdrawn_date}.")
                return visit, "discontinued", withdrawn_date
        else:
            if debug:
                print(f"{discontinued_variable} not found in {visit}.")

    return None


def get_subject_removed_status(
    config_file: Path, subject_id: str, visit_order: List[str]
) -> Dict[str, Any]:
    """
    Get the removed status of a subject.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        visit_order (List[str]): List of visits in the order they were conducted.

    Returns:
        Dict[str, Any]: Dictionary containing the removed status of the subject.
    """
    removed_r = check_if_removed(
        subject_id=subject_id, visit_order=visit_order, config_file=config_file
    )

    if removed_r is None:
        removed_event = np.nan
        removed_reason = np.nan
        removed = False
        withdrawn_date = np.nan
    else:
        removed_event, removed_reason, withdrawn_date = removed_r
        removed = True

    recruitment_status = data.get_subject_recruitment_status(
        config_file=config_file, subject_id=subject_id
    )

    if not removed:
        withdrawal_status = "not_withdrawn"
    else:
        if removed_event == "screening":
            if recruitment_status == "consented":
                withdrawal_status = "withdrawn_before_screening_outcome"
            else:
                withdrawal_status = f"withdrawn_before_baseline_{recruitment_status}"
        else:
            withdrawal_status = "withdrawn_after_baseline"

    subject_data = {
        "subject_id": subject_id,
        "removed": removed,
        "removed_date": withdrawn_date,
        "removed_event": removed_event,
        "removed_reason": removed_reason,
        "withdrawal_status": withdrawal_status,
    }

    return subject_data


def process_subject(params: Tuple[Path, str, List[str]]) -> Dict[str, Any]:
    """
    Wrapper function for the `get_subject_removed_status` function.

    Args:
        params (Tuple[Path, str, List[str]]): A tuple containing the config file, subject ID,
            and the visit order.

    Returns:
        Dict[str, Any]: Dictionary containing the removed status of the subject.
    """
    config_file, subject_id, visit_order = params
    subject_data = get_subject_removed_status(
        config_file=config_file, subject_id=subject_id, visit_order=visit_order
    )

    return subject_data


def compute_removed(config_file: Path, visit_order: List[str]) -> pd.DataFrame:
    """
    For each subject, compute if they have been removed.

    Args:
        config_file (Path): Path to the config file.
        visit_order (List[str]): List of visits in the order they were conducted.

    Returns:
        pd.DataFrame: DataFrame containing the removed status of each subject.
    """
    logger.info("Computing if subjects got removed...")

    subject_ids = data.get_all_subjects(config_file=config_file)
    subjects_count = len(subject_ids)
    logger.info(f"Found {subjects_count} subjects.")

    num_processes = 8
    logger.info(f"Using {num_processes} processes.")

    params = [(config_file, subject_id, visit_order) for subject_id in subject_ids]
    results = []

    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for result in pool.imap_unordered(process_subject, params):
                results.append(result)
                progress.update(task, advance=1)

    logger.info(f"Done computing removed status for {subjects_count} subjects.")

    removed_df = pd.DataFrame(results)

    return removed_df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    REMOVED_COUNT = count_removed(config_file=config_file)
    logger.info(f"Found {REMOVED_COUNT} subjects with removed status.")

    converted_status_df = compute_removed(
        config_file, visit_order=constants.visit_order
    )

    # commit_removed_status_to_db(config_file, converted_status_df)
    logger.info("Committing subject_removed table to the database...")
    db.df_to_table(
        config_file=config_file, df=converted_status_df, table_name="subject_removed"
    )
    UPDATED_REMOVED_COUNT = count_removed(config_file=config_file)

    logger.info(f"Found {UPDATED_REMOVED_COUNT} subjects with removed status.")
    logger.info(f"Added {UPDATED_REMOVED_COUNT - REMOVED_COUNT} subjects.")

    logger.info("Done!")
