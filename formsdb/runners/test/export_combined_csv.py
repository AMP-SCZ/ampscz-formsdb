#!/usr/bin/env python
"""
Export the combined CSVs.
Master CSVs with all the data in a single file.

Note: This script takes a long time to run.
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
from typing import Any, Dict, List, Tuple, Optional

import duckdb
import pandas as pd
from rich.logging import RichHandler
from rich.progress import Progress

from formsdb import constants, data
from formsdb.helpers import db, dpdash, utils

MODULE_NAME = "formsdb.runners.export.export_combined_csv"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

conn = duckdb.connect(database=":memory:")

additional_cols = [
    {
        "table": "subject_visit_status",
        "name": ["visit_started"],
        "column": ["timepoint"],
    },
    {
        "table": "subject_visit_completed",
        "name": ["visit_completed"],
        "column": ["completed_timepoint"],
    },
    {
        "table": "conversion_status",
        "name": ["converted", "converted_visit"],
        "column": ["converted", "converted_visit"],
    },
    {
        "table": "subject_removed",
        "name": ["removed", "removed_visit"],
        "column": ["removed", "removed_event"],
    },
    {
        "table": "recruitment_status",
        "name": ["recruited", "recruitment_status", "recruitment_status_v2"],
        "column": ["recruited", "recruitment_status", "recruitment_status_v2"],
    },
    {"table": "filters", "name": ["gender", "cohort"], "column": ["gender", "cohort"]},
]


def add_additional_cols(df: pd.DataFrame, config_file: Path) -> duckdb.DuckDBPyRelation:
    """
    Adds additional columns to the master table.

    Uses the additional_cols list from the top of the script.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        None
    """
    master_df = pd.DataFrame(df)
    logger.info("Adding additional columns to the master table...")
    for col in additional_cols:
        names = col["name"]
        table = col["table"]
        columns = col["column"]

        query = """
        SELECT
            subject_id,"""
        for name, column in zip(names, columns):
            query += f"{column} AS {name},"

        query = query[:-1]
        query += f"""
        FROM {table}
        """

        if table == "filters":
            query = query.replace("subject_id", "subject as subject_id")

        exec(f"{table}_df = db.execute_sql(config_file=config_file, query=query)")

        duckdb_query = "SELECT * FROM master_df"

    logger.info("Joining additional columns to the master table...")
    for col in additional_cols:
        df_name = f"{col['table']}_df"
        duckdb_query = f"""{duckdb_query}
        LEFT JOIN {df_name} USING (subject_id)"""

    logger.debug(f"DuckDB Query: {duckdb_query}")

    ddb_rel = duckdb.sql(duckdb_query, connection=conn)
    return ddb_rel


def export_data_to_csv(config_file: Path, event_name: str, df: pd.DataFrame) -> None:
    """
    Exports the data from DuckDB to a CSV file.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        None
    """
    ddb_rel = add_additional_cols(df=df, config_file=config_file)
    query = """
    SELECT
        *
    FROM
        ddb_rel
    """

    df = duckdb.execute(query, connection=conn).fetch_df()

    # Drop columns with all NaN values
    df.dropna(axis=1, how="all", inplace=True)

    # replace all occurrences of '.0' in values with ''
    df = df.astype(str).replace(r"\.0", "", regex=True)

    # replace None with pd.NA
    df = df.astype(str).replace("None", pd.NA)

    output_name = dpdash.get_dpdash_name(
        study="AMPSCZ",
        subject="combined",
        data_type="form",
        category=event_name,
        time_range="day1to1",
    )

    output_path = Path(f"{output_name}.csv")
    df.to_csv(output_path, index=False)
    logger.debug(f"Data exported to: {output_path}")
    return


def fetch_fasting_time(
    config_file: Path, subject_id: str, timepoint: str
) -> Optional[str]:
    """
    Returns the fasting time for a subject at a specific timepoint.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        timepoint (str): Timepoint.

    Returns:
        Optional[str]: Fasting time in HH:MM:SS format.
    """
    fasting_query = f"""
        SELECT
            time_fasting
        FROM
            subject_time_fasting
        WHERE
            subject_id = '{subject_id}' AND
            event_name = '{timepoint}'
        """

    fasting_time = db.fetch_record(config_file=config_file, query=fasting_query)

    return fasting_time


def fetch_vial_count(
    config_file: Path, subject_id: str, event_name: str, fluid_type: str
) -> int:
    """
    Fetches the number of vials collected for a specific fluid type.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        event_name (str): Event name.
        fluid_type (str): Fluid type. Either 'blood' or 'saliva'.

    Returns:
        int: Number of vials collected.
    """

    fetch_vial_count_query = f"""
    SELECT
        {fluid_type}_vial_count
    FROM
        subject_vials_count
    WHERE
        subject_id = '{subject_id}' AND
        timepoint = '{event_name}'
    """

    vial_count = db.fetch_record(config_file=config_file, query=fetch_vial_count_query)

    if vial_count is None:
        return 0

    vial_count = int(vial_count)

    return vial_count


def get_subject_visit_combined_df(
    config_file: Path, subject_id: str, event_name: str
) -> Dict[str, Any]:
    query = f"""
    SELECT
        subject_id,
        event_name,
        form_data
    FROM
        forms
    WHERE
        subject_id = '{subject_id}' AND
        event_name LIKE '%%{event_name}%%'
    """

    subject_df = db.execute_sql(config_file=config_file, query=query)

    subject_df = utils.explode_col(df=subject_df, col="form_data")

    try:
        merged_row = subject_df.iloc[0]
    except IndexError:
        return {}
    for i in range(1, len(subject_df)):
        merged_row = merged_row.combine_first(subject_df.iloc[i])

    # Include fasting and vials counts for baseline and month_2
    fluid_collection_timepoints: List[str] = ["baseline", "month_2"]
    if event_name in fluid_collection_timepoints:
        fasting_time = fetch_fasting_time(
            config_file=config_file, subject_id=subject_id, timepoint=event_name
        )
        merged_row["time_fasting"] = fasting_time

        for fluid in ["blood", "saliva"]:
            vial_count = fetch_vial_count(
                config_file=config_file,
                subject_id=subject_id,
                event_name=event_name,
                fluid_type=fluid,
            )

            merged_row[f"{fluid}_vial_count"] = vial_count

    # result_df = pd.DataFrame([merged_row])
    merged_row_dict = merged_row.to_dict()

    return merged_row_dict


def process_subject_visit(params: Tuple[Path, str, str]) -> Dict[str, Any]:
    config_file, subject_id, event_name = params
    return get_subject_visit_combined_df(
        config_file=config_file, subject_id=subject_id, event_name=event_name
    )


def get_visit_df(
    config_file: Path, event_name: str, progress: Progress
) -> pd.DataFrame:
    """
    Fetches the data for a specific event.

    Args:
        config_file (Path): Path to the config file.
        event_name (str): Name of the event.

    Returns:
        pd.DataFrame: Data for the event.
    """
    subject_ids = data.get_all_subjects(config_file=config_file)
    task_subjects = progress.add_task(
        f"Processing subjects for visit {event_name}...", total=len(subject_ids)
    )

    params = [(config_file, subject_id, event_name) for subject_id in subject_ids]
    results: List[Dict[str, Any]] = []

    with multiprocessing.Pool() as pool:
        for result in pool.imap_unordered(process_subject_visit, params):  # type: ignore
            # if non empty result add to results
            if result:
                results.append(result)
            progress.update(task_subjects, advance=1)

    progress.remove_task(task_subjects)
    concat_task = progress.add_task(f"Concating {event_name} results...", total=None)
    visit_df = pd.DataFrame(results, dtype=str)
    progress.remove_task(concat_task)

    return visit_df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    visits = constants.visit_order

    # duckdb.execute("SET GLOBAL pandas_analyze_sample=1000000")  # type: ignore

    with utils.get_progress_bar() as progress:
        task_visits = progress.add_task("Processing visits...", total=len(visits))
        for visit in visits:
            progress.update(task_visits, description=f"Processing visit {visit}...")
            visit_df = get_visit_df(
                config_file=config_file, event_name=visit, progress=progress
            )

            # export data to csv
            logger.info("Exporting data to CSV...")
            export_data_to_csv(config_file=config_file, event_name=visit, df=visit_df)

    logger.info("Done.")
