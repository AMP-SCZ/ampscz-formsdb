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
    query = "SELECT COUNT(*) FROM forms_derived.subject_removed WHERE removed = 'True';"
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

    Looks in the missing_data forms for:
    - chrmiss_withdrawn
    - chrmiss_discon
    - chrmiss_time_spec (A9, A10)
    - chrmiss_withdrawn_spec (withdrawn reason)
    - chrmiss_discon_spec (discontinued reason)

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

        missing_assesment_variable = "chrmiss_time_spec"
        if missing_assesment_variable in form_variables:
            if form_data[missing_assesment_variable] == "A9":
                withdrawn_date = data.estimate_event_date(
                    subject_id=subject_id, event=visit, config_file=config_file
                )
                return visit, "withdrawn", withdrawn_date
            if form_data[missing_assesment_variable] == "A10":
                withdrawn_date = data.estimate_event_date(
                    subject_id=subject_id, event=visit, config_file=config_file
                )
                return visit, "discontinued", withdrawn_date

        withdrawn_reason_variable = "chrmiss_withdrawn_spec"
        if withdrawn_reason_variable in form_variables:
            withdrawn_reason = form_data[withdrawn_reason_variable]
            if not pd.isna(withdrawn_reason):
                withdrawn_date = data.estimate_event_date(
                    subject_id=subject_id, event=visit, config_file=config_file
                )
                if debug:
                    print(f"Subject withdrew at {visit} on {withdrawn_date}.")
                return visit, withdrawn_reason, withdrawn_date

        discontinued_reason_variable = "chrmiss_discon_spec"
        if discontinued_reason_variable in form_variables:
            discontinued_reason = form_data[discontinued_reason_variable]
            if not pd.isna(discontinued_reason):
                withdrawn_date = data.estimate_event_date(
                    subject_id=subject_id, event=visit, config_file=config_file
                )
                if debug:
                    print(f"Subject discontinued at {visit} on {withdrawn_date}.")
                return visit, discontinued_reason, withdrawn_date

    return None


def check_if_removed_rpms(
    subject_id: str,
    visit_order: List[str],
    visit_mapping: Dict[str, str],
    config_file: Path,
) -> Optional[Tuple[str, str, Optional[datetime]]]:
    """
    Check if a subject has been removed.

    Uses the rpms_client_status table to check if the subject has been withdrawn, discontinued or
    Lost to Follow-up.
    Only applicable if the subject uses RPMS.

    Args:
        subject_id (str): Subject ID.
        config_file (Path): Path to the config file.
        visit_order (List[str]): List of visits in the order they were conducted.
        visit_mapping (Dict[str, str]): Mapping of visit names to db acceptable names.

    Returns:
        Optional[Tuple[str, str]]: If the subject was removed, returns the event, reason and date.
    """

    query = f"""
        SELECT * FROM forms.rpms_client_status WHERE subject_id = '{subject_id}'
    """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return None

    if df["Withdrawn"][0] is not None:
        withdrawn_reason = "withdrawn"
        withdrawn_date = df["Withdrawn"][0]
    elif df["Discontinued"][0] is not None:
        withdrawn_reason = "discontinued"
        withdrawn_date = df["Discontinued"][0]
    elif df["Lost to follow-up"][0] is not None:
        withdrawn_reason = "lost_to_follow_up"
        withdrawn_date = df["Lost to follow-up"][0]
    else:
        return None

    withdrawn_visit = None

    for visit in visit_order:
        if visit == "screening":
            # RPMS does not have a date recorded for screening
            continue

        # Get visit name from mapping:
        # Visit order uses DB names, we need to get actual names from the mapping
        #   mapping: Actual(key) -> DB(value)
        # Actual names are used in the client_status table
        # Actual names are the keys in the mapping
        def get_key_from_value(dictionary: Dict[str, str], value: str):
            return next((key for key, val in dictionary.items() if val == value), None)

        visit_name = get_key_from_value(visit_mapping, visit)

        if df[visit_name][0] is not None:
            withdrawn_visit = visit_name

    if withdrawn_visit:
        withdrawn_visit = visit_mapping[withdrawn_visit]

    if withdrawn_visit is None and withdrawn_date is not None:
        # Since the withdrawn date is not associated with a visit, we assume it is from screening
        withdrawn_visit = "screening"

    return withdrawn_visit, withdrawn_reason, withdrawn_date  # type: ignore


def check_if_removed_redcap(
    subject_id: str, config_file: Path
) -> Optional[Tuple[str, str, Optional[datetime]]]:
    """
    Checks for statusform_withdrawal on floating_forms (statusform)

    Returns:
        Optional[Tuple[str, str, Optional[datetime]]]: If the subject was removed,
            returns the event, reason and date. Else, returns None.
    """
    variable = "statusform_withdrawal"
    variable_name = "withdrawal_date"
    form_names = ["status_form", "uncategorized"]
    event_name = "floating_forms"

    # Legacy Form
    for form_name in form_names:
        query = f"""
        SELECT
            CASE
                WHEN LENGTH(form_data ->> '{variable}') >= 10 THEN
                    TO_DATE(form_data ->> '{variable}', 'YYYY-MM-DD')
                ELSE
                    NULL
            END AS {variable_name}
        FROM
            forms.forms
        WHERE
            subject_id = '{subject_id}'
            AND form_name = '{form_name}'
            AND event_name LIKE '%%{event_name}%%'
            AND form_data ? '{variable}';
        """

        result = db.fetch_record(config_file=config_file, query=query)

        if result is not None:
            break

    # New Form
    if result is None:
        variable = "chr_subject_date_of_withdrawal"
        form_name = "revised_status_form"
        event_name = "floating_forms"

        query = f"""
        SELECT
            TO_DATE(form_data ->> '{variable}', 'YYYY-MM-DD') AS {variable_name}
        FROM
            forms.forms
        WHERE
            subject_id = '{subject_id}'
            AND form_name = '{form_name}'
            AND event_name LIKE '%%{event_name}%%'
            AND form_data ? '{variable}';
        """

        result = db.fetch_record(config_file=config_file, query=query)

    if result is None:
        return None
    else:
        try:
            result_dt = datetime.strptime(result, "%Y-%m-%d")
            withdrawn_timepoint = data.get_closest_timepoint(
                config_file=config_file, subject_id=subject_id, date=result_dt
            )
            if withdrawn_timepoint is not None:
                return withdrawn_timepoint, "withdrawn", result_dt
        except ValueError:
            logger.error(f"{subject_id}: Could not parse date: {result}")
            return None

    return "uncategorized", "withdrawn", result_dt


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
    removed_info_source = None

    # If the subject uses RPMS, check the client_status table
    # Else use all forms to determing if the subject was removed
    # If still not withdrawn, check the statusform_withdrawal on floating_forms
    if data.subject_uses_rpms(config_file=config_file, subject_id=subject_id):
        removed_r = check_if_removed_rpms(
            subject_id=subject_id,
            visit_order=visit_order,
            visit_mapping=constants.client_status_visit_mapping,
            config_file=config_file,
        )
        removed_info_source = "client_status"
    else:
        removed_r = check_if_removed(
            subject_id=subject_id, visit_order=visit_order, config_file=config_file
        )
        removed_info_source = "missing_data_form"

        if removed_r is None:
            removed_r = check_if_removed_redcap(
                subject_id=subject_id, config_file=config_file
            )
            removed_info_source = "floating_forms(statusform_withdrawal)"

    if removed_r is None:
        removed_event = np.nan
        removed_reason = np.nan
        removed = False
        withdrawn_date = np.nan
        removed_info_source = None
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
        "removed_info_source": removed_info_source,
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
        config_file=config_file,
        df=converted_status_df,
        schema="forms_derived",
        table_name="subject_removed",
    )
    UPDATED_REMOVED_COUNT = count_removed(config_file=config_file)

    logger.info(f"Found {UPDATED_REMOVED_COUNT} subjects with removed status.")
    logger.info(f"Added {UPDATED_REMOVED_COUNT - REMOVED_COUNT} subjects.")

    logger.info("Done!")
