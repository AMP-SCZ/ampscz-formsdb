#!/usr/bin/env python
"""
Infers the visit status of subjects based on the most recent visit form completed.
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
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from rich.logging import RichHandler
from rich.table import Table

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


def get_vist_status_stats(config_file: Path) -> Dict[str, int]:
    """
    For each visit status, count the number of subjects with that visit status.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        Dict[str, int]: A dictionary with visit status as the key and the count as the value.
    """
    query = """
    SELECT subject_visit_status, COUNT(*) AS count
    FROM subject_visit_status
    GROUP BY subject_visit_status;
    """
    engine = db.get_db_connection(config_file=config_file)
    df = pd.read_sql(query, engine)

    visit_status_stats = {}
    for _, row in df.iterrows():
        visit_status = row["subject_visit_status"]
        count = row["count"]
        visit_status_stats[visit_status] = count

    engine.dispose()

    return visit_status_stats


def print_visit_status_stats(
    config_file: Path, compared_to_visit_status_map: Optional[Dict[str, int]] = None
) -> None:
    """
    Print the visit status stats, as a table.

    Args:
        config_file (Path): Path to the config file.
        compared_to_visit_status_map (Optional[Dict[str, int]], optional): A dictionary with visit
            status as the key and the count as the value. Defaults to None.

    Returns:
        None
    """
    visit_status_stats = get_vist_status_stats(config_file)

    visit_order = constants.visit_order

    table = Table(title="Visit Status Stats")
    table.add_column("Visit Status", justify="left", style="cyan", no_wrap=True)
    table.add_column("Count", justify="right", style="magenta")

    if compared_to_visit_status_map is not None:
        table.add_column("Change", justify="right", style="bold green")

    for visit in visit_order:
        try:
            count = visit_status_stats[visit]
        except KeyError:
            count = 0
        if compared_to_visit_status_map is not None:
            try:
                compared_to_count = compared_to_visit_status_map[visit]
            except KeyError:
                compared_to_count = 0
            change = count - compared_to_count
            if change > 0:
                change = f"+{change}"
            else:
                change = f"{change}"
            table.add_row(visit, str(count), str(change))
        else:
            table.add_row(visit, str(count))

    console.print(table)


def compute_recent_visit_rpms(
    df: pd.DataFrame, visit_order: List[str], debug: bool = False
) -> str:
    """
    Infers the most recent visit of a subject based on the most recent form completed.

    Args:
        df (pd.DataFrame): DataFrame containing the forms data of a subject.
        visit_order (List[str]): List of visits in the order they were conducted.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        str: The most recent visit of the subject.
    """
    most_recent_visit = "None"

    for visit in visit_order:
        visit_df = df[df["event_name"].str.contains(f"{visit}_")]

        if visit_df.shape[0] == 0:
            continue

        for _, row in visit_df.iterrows():
            form_name = row["form_name"]

            if form_name == "sociodemographics":
                continue

            form_data_r: Dict[str, Any] = row["form_data"]
            form_variables = form_data_r.keys()

            date_variables = [v for v in form_variables if "date" in v]

            for date_variable in date_variables:
                date = form_data_r[date_variable]

                # Validate date
                if not utils.validate_date(date):
                    if debug:
                        print(f"Invalid date: {date}")
                    continue
                else:
                    date = pd.to_datetime(date)
                    if date < datetime(2019, 1, 1):
                        continue
                    if debug:
                        print(f"{visit} {date_variable}: {date}")
                    most_recent_visit = visit

    return most_recent_visit


def compute_recent_visit_redcap(
    df: pd.DataFrame, visit_order: List[str], debug: bool = False
) -> str:
    """
    Compute the most recent visit of a subject based on the most recent form completed.

    Ignore forms that contain digital_biomarkers, as they are pre-filled and could
    be misleading.

    Args:
        df (pd.DataFrame): DataFrame containing the forms data of a subject.
        visit_order (List[str]): List of visits in the order they were conducted.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        str: The most recent visit of the subject.
    """
    most_recent_visit = "None"

    for visit in visit_order:
        visit_df = df[df["event_name"].str.contains(f"{visit}_")]
        metadata_df = visit_df[visit_df["form_name"] == "uncategorized"]
        if metadata_df.shape[0] == 0:
            continue
        metadata_r = metadata_df.iloc[0, :]
        metadata: Dict[str, Any] = metadata_r["form_data"]

        form_variables = list(metadata.keys())

        complete_variables = [
            variable for variable in form_variables if variable.endswith("_complete")
        ]

        if debug:
            print(f"{visit}: {len(complete_variables)}")
            print(complete_variables)
            for complete_variable in complete_variables:
                print(f"{complete_variable}: {metadata[complete_variable]}")

        # Skip variables that contain digital_biomarkers
        complete_variables = [
            variable
            for variable in complete_variables
            if "digital_biomarkers" not in variable
        ]

        complete_count = 0
        for complete_variable in complete_variables:
            if metadata[complete_variable] == 2:
                complete_count += 1

        if complete_count >= 0.8 * len(complete_variables):
            most_recent_visit = visit

        if debug:
            print(f"{visit}: {complete_count} / {len(complete_variables)}")
            print()

    return most_recent_visit


def compute_recent_visit(config_file: Path, debug: bool = False) -> pd.DataFrame:
    """
    Compute the most recent visit of each subject.

    Args:
        config_file (Path): Path to the config file.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame containing the most recent visit of each subject.
    """
    visit_status_df = pd.DataFrame(columns=["subject_id", "visit_status"])
    logger.info("Computing each subject's most recent visit...")

    query = "SELECT COUNT(*) FROM subjects;"
    subject_count_r = db.fetch_record(config_file=config_file, query=query)
    if subject_count_r is None:
        raise ValueError("No subjects found.")
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
            visit_status = compute_recent_visit_rpms(
                df, constants.visit_order, debug=debug
            )

            visit_status_df = pd.concat(
                [
                    visit_status_df,
                    pd.DataFrame(
                        {
                            "subject_id": [subject_id],
                            "visit_status": [visit_status],
                        }
                    ),
                ]
            )

    logger.info(f"Done computing visit status for {subject_count} subjects.")

    return visit_status_df


def commit_visit_status_to_db(config_file: Path, df: pd.DataFrame) -> None:
    """
    Drops the existing subject_visit_status table and commits the new visit status data.

    Args:
        config_file (Path): Path to the config file.
        df (pd.DataFrame): DataFrame containing the visit status of each subject.

    Returns:
        None
    """
    logger.info("Committing visit status to the database...")

    sql_queries = []
    # Remove existing data
    sql_query = """
    DELETE FROM subject_visit_status;
    """
    sql_queries.append(sql_query)

    for _, row in df.iterrows():
        subject_id = row["subject_id"]
        visit_status = row["visit_status"]
        sql_query = f"""
        INSERT INTO subject_visit_status (subject_id, subject_visit_status)
        VALUES ('{subject_id}', '{visit_status}');
        """
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

    visit_status_stats = get_vist_status_stats(config_file)
    logger.info("Visit status stats:")
    print_visit_status_stats(config_file=config_file)

    visit_status_df = compute_recent_visit(config_file, debug=False)

    commit_visit_status_to_db(config_file, visit_status_df)

    logger.info("Updated visit status stats:")
    print_visit_status_stats(
        config_file=config_file, compared_to_visit_status_map=visit_status_stats
    )

    logger.info("Done!")
