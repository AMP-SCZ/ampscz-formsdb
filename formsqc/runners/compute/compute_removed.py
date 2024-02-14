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
    if parent.name == "ampscz-formsqc":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from rich.logging import RichHandler

from formsqc import constants, data
from formsqc.helpers import db, utils

MODULE_NAME = "formsqc_compute_removed"

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
    query = "SELECT COUNT(*) FROM subject_removed WHERE subject_removed = 'True';"
    removed_count_r = db.fetch_record(config_file=config_file, query=query)
    if removed_count_r is None:
        raise ValueError("No removed subjects found in the database.")
    removed_count = int(removed_count_r)

    return removed_count


def check_if_removed(
    df: pd.DataFrame, visit_order: List[str], debug: bool = False
) -> Optional[Tuple[str, str]]:
    """
    Check if a subject has been removed.

    Args:
        df (pd.DataFrame): DataFrame containing the subject's forms.
        visit_order (List[str]): List of visits in the order they were conducted.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        Optional[Tuple[str, str]]: If the subject was removed, returns the event and reason.
    """
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
                if debug:
                    print(f"Subject withdrew at {visit}.")
                return visit, "withdrawn"
        else:
            if debug:
                print(f"{withdrawn_variable} not found in {visit}.")

        discontinued_variable = "chrmiss_discon"
        if discontinued_variable in form_variables:
            if form_data[discontinued_variable] == 1:
                if debug:
                    print(f"Subject discontinued at {visit}.")
                return visit, "discontinued"
        else:
            if debug:
                print(f"{discontinued_variable} not found in {visit}.")

    return None


def compute_removed(
    config_file: Path, visit_order: List[str], debug: bool = False
) -> pd.DataFrame:
    """
    For each subject, compute if they have been removed.

    Args:
        config_file (Path): Path to the config file.
        visit_order (List[str]): List of visits in the order they were conducted.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame containing the removed status of each subject.
    """
    removed_df = pd.DataFrame(
        columns=["subject_id", "removed", "removed_event", "removed_reason"]
    )
    logger.info("Computing if subjects got removed...")

    query = "SELECT COUNT(*) FROM subjects;"
    subject_count_r = db.fetch_record(config_file=config_file, query=query)
    if subject_count_r is None:
        raise ValueError("No subjects found in the database.")
    subject_count = int(subject_count_r)

    logger.info(f"Found {subject_count} subjects.")

    query = "SELECT id FROM subjects ORDER BY id;"
    engine = db.get_db_connection(config_file=config_file)

    subject_ids_r = pd.read_sql(query, engine, chunksize=1)

    with utils.get_progress_bar() as progress:
        task = progress.add_task("[red]Processing...", total=subject_count)

        for row in subject_ids_r:
            subject_id = row["id"].values[0]

            progress.update(task, advance=1, description=f"Processing {subject_id}...")

            df = data.get_all_subject_forms(
                config_file=config_file, subject_id=subject_id
            )
            removed_r = check_if_removed(df, visit_order, debug=debug)
            if removed_r is None:
                removed_event = np.nan
                removed_reason = np.nan
                removed = False
            else:
                removed_event, removed_reason = removed_r
                removed = True

            removed_df = pd.concat(
                [
                    removed_df,
                    pd.DataFrame(
                        {
                            "subject_id": [subject_id],
                            "removed": [removed],
                            "removed_event": [removed_event],
                            "removed_reason": [removed_reason],
                        }
                    ),
                ]
            )

    logger.info(f"Done computing removed status for {subject_count} subjects.")

    return removed_df


def commit_removed_status_to_db(config_file: Path, df: pd.DataFrame) -> None:
    """
    Drop old subject_removed data and commit new data to the database.

    Args:
        config_file (Path): Path to the config file.
        df (pd.DataFrame): DataFrame containing the removed status of each subject.

    Returns:
        None
    """
    logger.info("Committing removed status to the database...")

    sql_queries: List[str] = []
    # Remove existing data
    sql_query = """
    DELETE FROM subject_removed;
    """
    sql_queries.append(sql_query)

    for _, row in df.iterrows():
        subject_id = row["subject_id"]
        removed = row["removed"]
        removed_event = row["removed_event"]
        removed_reason = row["removed_reason"]

        if pd.isna(removed_event):
            removed_event = "NULL"
        if pd.isna(removed_reason):
            removed_reason = "NULL"

        sql_query = f"""
        INSERT INTO subject_removed (subject_id, subject_removed, subject_removed_event, subject_removed_reason)
        VALUES ('{subject_id}', '{removed}', '{removed_event}', '{removed_reason}');
        """
        sql_query = db.handle_null(sql_query)
        sql_queries.append(sql_query)

    logger.info("Removing existing data and committing new data...")
    db.execute_queries(
        config_file=config_file, queries=sql_queries, show_commands=False
    )


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
        config_file, visit_order=constants.visit_order, debug=False
    )

    commit_removed_status_to_db(config_file, converted_status_df)
    UPDATED_REMOVED_COUNT = count_removed(config_file=config_file)

    logger.info(f"Found {UPDATED_REMOVED_COUNT} subjects with removed status.")
    logger.info(f"Added {UPDATED_REMOVED_COUNT - REMOVED_COUNT} subjects.")

    logger.info("Done!")
