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
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd
from rich.logging import RichHandler
from rich.progress import Progress

from formsdb import constants, data
from formsdb.helpers import db, dpdash, utils

pd.options.mode.chained_assignment = None  # default='warn'

MODULE_NAME = "formsdb.runners.export.export_combined_csv"

console = utils.get_console()

error_cache: Set[str] = set()

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
        "name": ["visit_started", "visit_status", "visit_status_string"],
        "column": ["timepoint", "timepoint", "timepoint"],
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
        "name": ["removed", "removed_visit", "removed_date", "removed_info_source"],
        "column": ["removed", "removed_event", "removed_date", "removed_info_source"],
    },
    {
        "table": "recruitment_status",
        "name": ["recruited", "recruitment_status", "recruitment_status_v2"],
        "column": ["recruited", "recruitment_status", "recruitment_status_v2"],
    },
    {
        "table": "filters",
        "name": ["gender", "cohort", "age_at_consent"],
        "column": ["gender", "cohort", "age"],
    },
]


def legacy_add_additional_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds additional columns to the master table to match the legacy export.

    Adds the following columns:
    - visit_status_string
    - subjectid

    Args:
        df (pd.DataFrame): Dataframe to add additional columns to.
        config_file (Path): Path to the config file.

    Returns:
        pd.DataFrame: Dataframe with additional columns added.
    """

    df = df.copy()

    # Copy subject_id to subjectid column
    df["subjectid"] = df["subject_id"]

    # Add visit_status_string column based on the visit_status column, from the following mapping:
    # screening = screen
    # baseline = baseln
    # month_1 = month1
    # ...
    # month_24 = month24

    visit_status_mapping = {
        "screening": "screen",
        "baseline": "baseln",
        "month_1": "month1",
        "month_2": "month2",
        "month_3": "month3",
        "month_4": "month4",
        "month_5": "month5",
        "month_6": "month6",
        "month_7": "month7",
        "month_8": "month8",
        "month_9": "month9",
        "month_10": "month10",
        "month_11": "month11",
        "month_12": "month12",
        "month_18": "month18",
        "month_24": "month24",
    }

    df["visit_status_string"] = df["visit_status"].map(visit_status_mapping)

    # replace visit_status_string with 'removed' if removed is True
    df.loc[df["removed"] == "True", "visit_status_string"] = "removed"

    # replace visit_status_string with 'converted' if converted is True
    df.loc[df["converted"] == "True", "visit_status_string"] = "converted"

    return df


def add_additional_cols(
    df: pd.DataFrame, config_file: Path  # noqa # pylint: disable=unused-argument
) -> duckdb.DuckDBPyRelation:
    """
    Adds additional columns to the master table.

    Uses the additional_cols list from the top of the script.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        None
    """
    master_df = pd.DataFrame(df)  # noqa # pylint: disable=unused-variable
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

        exec(  # pylint: disable=exec-used
            f"{table}_df = db.execute_sql(config_file=config_file, query=query)"
        )

        duckdb_query = "SELECT * FROM master_df"

    logger.info("Joining additional columns to the master table...")
    for col in additional_cols:
        df_name = f"{col['table']}_df"
        duckdb_query = f"""{duckdb_query}
        LEFT JOIN {df_name} USING (subject_id)"""

    logger.debug(f"DuckDB Query: {duckdb_query}")

    ddb_rel = duckdb.sql(duckdb_query, connection=conn)
    return ddb_rel


def get_combined_csvs_output_dir(config_file: Path) -> Path:
    """
    Get the output directory for the combined CSVs.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["combined_csvs"])
    return output_dir


# Reference:
# https://github.com/AMP-SCZ/utility/blob/15a5ef5b49d1e081ee0a549375f78bb26160d958/rpms_to_redcap.py#L55C1-L71C21
def handle_datetime(time_value: str) -> datetime:
    """
    Handles different time formats from RPMS.

    This helps standardize the time formats to be used in the exported CSVs.

    Args:
        time_value (str): Time value to handle.

    Returns:
        datetime: Time value in datetime format.
    """
    if len(time_value) == 10:
        try:
            # interview_date e.g. 11/30/2022
            datetime_val = datetime.strptime(time_value, "%m/%d/%Y")
        except ValueError:
            # psychs form e.g. 03/03/1903
            datetime_val = datetime.strptime(time_value, "%d/%m/%Y")
    elif len(time_value) > 10:
        # all other forms e.g. 1/05/2022 12:00:00 AM
        datetime_val = datetime.strptime(time_value, "%d/%m/%Y %I:%M:%S %p")
    else:
        raise ValueError(f"Unknown time format: {time_value}")

    return datetime_val


def cast_dates_to_str(
    data_df: pd.DataFrame, config_file: Path, network: str
) -> pd.DataFrame:
    """
    Casts date columns to string.

    date_ymd - date in YYYY-MM-DD format
    datetime_ymd - datetime in YYYY-MM-DD HH:MM format

    Args:
        df (pd.DataFrame): Dataframe to cast date columns to string.
        config_file (Path): Path to the config file.

    Returns:
        pd.DataFrame: Dataframe with date columns cast to string.
    """

    data_dictionary_df = data.get_data_dictionary(config_file=config_file)

    dates_df = data_dictionary_df[data_dictionary_df["text_validation_type_or_show_slider_number"] == "date_ymd"]
    datetime_df = data_dictionary_df[
        data_dictionary_df["text_validation_type_or_show_slider_number"] == "datetime_ymd"
    ]
    time_df = data_dictionary_df[data_dictionary_df["text_validation_type_or_show_slider_number"] == "time"]

    date_variables = dates_df["field_name"].tolist()

    logger.debug("Casting dates/times/datetimes to REDCap native format...")
    for date_variable in date_variables:
        if date_variable not in data_df.columns:
            continue
        # data_df[date_variable] = pd.to_datetime(data_df[date_variable], errors="ignore")
        # cast to string "YYYY-MM-DD"
        for idx, row in data_df.iterrows():
            date_raw_val = row[date_variable]
            if not pd.isnull(date_raw_val) and date_raw_val is not None:
                try:
                    if network == "PRESCIENT":
                        date_val = handle_datetime(date_raw_val)  # type: ignore
                    else:
                        date_val = datetime.fromisoformat(date_raw_val)  # type: ignore
                    date_str = date_val.strftime("%Y-%m-%d")
                    data_df.at[idx, date_variable] = date_str
                except (TypeError, AttributeError):
                    error_message = (
                        f"date cast failed ({date_variable}): {date_raw_val}"
                    )
                    if error_message not in error_cache:
                        logger.error(error_message)
                        error_cache.add(error_message)
                except ValueError:
                    pass

    datetime_variables = datetime_df["field_name"].tolist()

    for datetime_variable in datetime_variables:
        if datetime_variable not in data_df.columns:
            continue
        # data_df[datetime_variable] = pd.to_datetime(
        #     data_df[datetime_variable], errors="ignore"
        # )
        # cast to string "YYYY-MM-DD HH:MM"
        for idx, row in data_df.iterrows():
            datetime_val = row[datetime_variable]
            if not pd.isnull(datetime_val) and datetime_val is not None:
                try:
                    if network == "PRESCIENT":
                        datetime_val = handle_datetime(datetime_val)  # type: ignore
                    else:
                        datetime_val = datetime.fromisoformat(datetime_val)  # type: ignore
                    datetime_str = datetime_val.strftime("%Y-%m-%d %H:%M")
                    data_df.at[idx, datetime_variable] = datetime_str
                except (TypeError, AttributeError):
                    error_message = (
                        f"datetime cast failed ({datetime_variable}): {datetime_val}"
                    )
                    if error_message not in error_cache:
                        logger.error(error_message)
                        error_cache.add(error_message)
                except ValueError:
                    pass
        # data_df[datetime_variable] = data_df[datetime_variable].dt.strftime("%Y-%m-%d %H:%M")

    time_variables = time_df["field_name"].tolist()

    for time_variable in time_variables:
        if time_variable not in data_df.columns:
            continue

        # cast to string "HH:MM"
        for idx, row in data_df.iterrows():
            time_val = row[time_variable]
            if not pd.isnull(time_val) and time_val is not None:
                try:
                    if network == "PRESCIENT":
                        time_val = handle_datetime(time_val)  # type: ignore
                    else:
                        time_val = datetime.fromisoformat(time_val)  # type: ignore
                    time_str = time_val.strftime("%H:%M")
                    data_df.at[idx, time_variable] = time_str
                except TypeError:
                    logger.error(f"time cast failed: {time_val}")
                except ValueError:
                    pass

    return data_df


def combine_data_from_formsdb(
    config_file: Path,
    network: str,
    event_name: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine the data from the formsdb database.

    Args:
        config_file (Path): Path to the config file.
        network (str): Network name.
        event_name (str): Event name.
        df (pd.DataFrame): Dataframe to export.

    Returns:
        pd.DataFrame: Dataframe with additional columns added.
    """
    ddb_rel = add_additional_cols(  # noqa # pylint: disable=unused-variable
        df=df, config_file=config_file
    )  # noqa # pylint: disable=unused-variable
    query = """
    SELECT
        *
    FROM
        ddb_rel
    """

    df = duckdb.execute(query, connection=conn).fetch_df()

    # Cast date columns to string
    # if network != 'PRESCIENT':
    #     df = cast_dates_to_str(data_df=df, config_file=config_file)
    df = cast_dates_to_str(data_df=df, config_file=config_file, network=network)

    # replace NaT with ''
    df = df.astype(str).replace("NaT", "")

    # Drop columns with all NaN values in data columns
    mandatory_cols = [
        "subject_id"
        "visit_started",
        "visit_status",
        "visit_status_string",
        "visit_completed",
        "converted",
        "converted_visit",
        "removed",
        "removed_visit",
        "removed_date",
        "removed_info_source",
        "recruited",
        "recruitment_status",
        "recruitment_status_v2",
        "gender",
        "cohort",
        "age_at_consent",
        "subjectid"
    ]

    data_cols = list(set(df.columns) - set(mandatory_cols))
    df.dropna(axis=0, how="all", subset=data_cols, inplace=True)
    df.dropna(axis=1, how="all", inplace=True)

    # replace all occurrences of '.0' in values with ''
    df = df.astype(str).replace(r"\.0+$", "", regex=True)

    # replace None with ''
    df = df.astype(str).replace("None", "")
    # df = df.replace(
    #     to_replace=[None],
    #     value=""
    # )
    # df = df.replace(
    #     to_replace=[pd.NA],
    #     value=""
    # )

    # Replace 'True' and 'False' with '1' and '0'
    df = df.astype(str).replace("True", "1")
    df = df.astype(str).replace("False", "0")

    return df


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
    """
    Fetches the data for a specific subject and event.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        event_name (str): Event name.

    Returns:
        Dict[str, Any]: Data for the subject and event.
    """
    query = f"""
    SELECT
        subject_id,
        event_name,
        form_data
    FROM
        forms
    WHERE
        subject_id = '{subject_id}' AND
        event_name LIKE '%%{event_name}_arm%%'
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

            # SettingWithCopyWarning: A value is trying to be set on a copy of a slice from a DataFrame.
            merged_row[f"{fluid}_vial_count"] = vial_count

    # result_df = pd.DataFrame([merged_row])
    merged_row_dict = merged_row.to_dict()

    return merged_row_dict


def process_subject_visit(params: Tuple[Path, str, str]) -> Dict[str, Any]:
    """
    Wrapper function to process a subject visit.

    Args:
        params (Tuple[Path, str, str]): Tuple containing the
            config file, subject ID, and event name.

    Returns:
        Dict[str, Any]: Data for the subject and event.
    """
    config_file, subject_id, event_name = params
    return get_subject_visit_combined_df(
        config_file=config_file, subject_id=subject_id, event_name=event_name
    )


def get_visit_df(
    config_file: Path, network: str, event_name: str, progress: Progress
) -> pd.DataFrame:
    """
    Fetches the data for a specific event.

    Args:
        config_file (Path): Path to the config file.
        network (str): Network name.
        event_name (str): Name of the event.

    Returns:
        pd.DataFrame: Data for the event.
    """
    subject_ids = data.get_all_subjects(config_file=config_file, network=network)
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

    output_dir = get_combined_csvs_output_dir(config_file=config_file)
    logger.info(f"Output directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    visits = constants.visit_order
    networks = constants.networks_legacy
    # reverse the order of networks
    networks = networks[::-1]

    visits = ["conversion", "floating_forms"] + visits

    # duckdb.execute("SET GLOBAL pandas_analyze_sample=1000000")  # type: ignore

    with utils.get_progress_bar() as progress:
        task_networks = progress.add_task("Processing networks...", total=len(networks))
        for network in networks:
            progress.update(
                task_networks, description=f"Processing network {network}..."
            )
            task_visits = progress.add_task("Processing visits...", total=len(visits))
            for visit in visits:
                progress.update(task_visits, description=f"Processing visit {visit}...")
                visit_df = get_visit_df(
                    config_file=config_file,
                    event_name=visit,
                    network=network,
                    progress=progress,
                )

                # export data to csv
                logger.info("Exporting data to CSV...")
                export_task = progress.add_task(
                    f"Exporting {visit} data...", total=None
                )
                visit_df = combine_data_from_formsdb(
                    config_file=config_file,
                    network=network,
                    event_name=visit,
                    df=visit_df,
                )

                # legacy
                visit_df = legacy_add_additional_cols(df=visit_df)

                output_name = dpdash.get_dpdash_name(
                    study="AMPSCZ",
                    subject="combined",
                    data_type="redcap",
                    category=visit,
                    optional_tag=[network],
                    time_range="day1to1",
                )

                output_path = output_dir / f"{output_name}.csv"
                visit_df.to_csv(output_path, index=False)
                logger.debug(f"Data exported to: {output_path}")

                progress.remove_task(export_task)
                progress.update(task_visits, advance=1)
            progress.update(task_networks, advance=1)

    logger.info("Done.")
