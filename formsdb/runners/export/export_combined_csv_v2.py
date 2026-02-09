#!/usr/bin/env python
"""
Export the combined CSVs.
Master CSVs with all the data in a single file.

Optimized: Uses bulk SQL queries instead of per-subject queries.
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
import warnings
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import sqlalchemy
from rich.logging import RichHandler
from rich.progress import Progress

from formsdb import constants, data
from formsdb.helpers import db, dpdash, utils

pd.options.mode.chained_assignment = None  # default='warn'

MODULE_NAME = "formsdb.runners.export.export_combined_csv"

console = utils.get_console()

error_cache: Set[str] = set()
data_dictionary_cache: Optional[pd.DataFrame] = None  # Cache for data dictionary
_engine: Optional[sqlalchemy.engine.base.Engine] = None  # Reusable DB engine

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

# Silence Pandas UserWarnings globally
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

additional_cols_config = [
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
        "name": ["converted", "converted_visit", "conversion_date"],
        "column": ["converted", "converted_visit", "conversion_date"],
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


def get_engine(config_file: Path) -> sqlalchemy.engine.base.Engine:
    """Get or create a reusable DB engine (avoids creating one per query)."""
    global _engine
    if _engine is None:
        _engine = db.get_db_connection(config_file)
    return _engine


def execute_sql_fast(config_file: Path, query: str) -> pd.DataFrame:
    """Execute SQL using the shared engine instead of creating a new one each time."""
    engine = get_engine(config_file)
    return pd.read_sql(query, engine)


def legacy_add_additional_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds additional columns to the master table to match the legacy export.

    Adds the following columns:
    - visit_status_string
    - subjectid

    Args:
        df (pd.DataFrame): Dataframe to add additional columns to.

    Returns:
        pd.DataFrame: Dataframe with additional columns added.
    """

    df = df.copy()

    # Copy subject_id to subjectid column
    df["subjectid"] = df["subject_id"]

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
    df.loc[df["removed"] == "1", "visit_status_string"] = "removed"

    # replace visit_status_string with 'converted' if converted is True
    df.loc[df["converted"] == "True", "visit_status_string"] = "converted"
    df.loc[df["converted"] == "1", "visit_status_string"] = "converted"

    return df


def fetch_additional_cols_bulk(config_file: Path) -> Dict[str, pd.DataFrame]:
    """
    Fetches ALL additional column tables in bulk (once), instead of per-visit.

    Returns:
        Dict[str, pd.DataFrame]: Mapping of table name -> DataFrame.
    """
    additional_dfs: Dict[str, pd.DataFrame] = {}

    for col_spec in additional_cols_config:
        table = col_spec["table"]
        names = col_spec["name"]
        columns = col_spec["column"]

        select_parts = ["subject_id"]
        for name, column in zip(names, columns):
            select_parts.append(f"{column} AS {name}")

        query = f"SELECT {', '.join(select_parts)} FROM forms_derived.{table}"

        if table == "filters":
            query = query.replace("subject_id", "subject AS subject_id")

        logger.info(f"Fetching additional columns from {table}...")
        additional_dfs[table] = execute_sql_fast(config_file=config_file, query=query)

    return additional_dfs


def add_additional_cols_fast(
    df: pd.DataFrame, additional_dfs: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Joins pre-fetched additional column DataFrames onto the master DataFrame
    using pandas merge (no exec(), no per-query DB calls).

    Args:
        df (pd.DataFrame): Master DataFrame.
        additional_dfs (Dict[str, pd.DataFrame]): Pre-fetched additional column tables.

    Returns:
        pd.DataFrame: DataFrame with additional columns joined.
    """
    result = df
    for col_spec in additional_cols_config:
        table = col_spec["table"]
        additional_df = additional_dfs[table]
        result = result.merge(additional_df, on="subject_id", how="left")

    return result


def get_combined_csvs_output_dir(config_file: Path) -> Path:
    """
    Get the output directory for the combined CSVs.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["combined_csvs"])
    return output_dir


def cast_dates_to_str(
    data_df: pd.DataFrame, config_file: Path, network: str
) -> pd.DataFrame:
    """
    Casts date columns to string using vectorized operations for performance.

    date_ymd - date in YYYY-MM-DD format
    datetime_ymd - datetime in YYYY-MM-DD HH:MM format

    Args:
        data_df (pd.DataFrame): Dataframe to cast date columns to string.
        config_file (Path): Path to the config file.
        network (str): Network name.

    Returns:
        pd.DataFrame: Dataframe with date columns cast to string.
    """
    global data_dictionary_cache

    # Use cached data dictionary if available
    if data_dictionary_cache is None:
        data_dictionary_cache = data.get_data_dictionary(config_file=config_file)

    data_dictionary_df = data_dictionary_cache

    dates_df = data_dictionary_df[
        data_dictionary_df["text_validation_type_or_show_slider_number"] == "date_ymd"
    ]
    datetime_df = data_dictionary_df[
        data_dictionary_df["text_validation_type_or_show_slider_number"]
        == "datetime_ymd"
    ]
    time_df = data_dictionary_df[
        data_dictionary_df["text_validation_type_or_show_slider_number"] == "time"
    ]

    # Only process columns that actually exist in the dataframe
    existing_cols = set(data_df.columns)
    date_variables = [v for v in dates_df["field_name"].tolist() if v in existing_cols]
    datetime_variables = [
        v for v in datetime_df["field_name"].tolist() if v in existing_cols
    ]
    time_variables = [v for v in time_df["field_name"].tolist() if v in existing_cols]

    logger.debug(
        f"Casting {len(date_variables)} dates, {len(datetime_variables)} datetimes, "
        f"{len(time_variables)} times to REDCap native format..."
    )

    # Vectorized date conversion
    for date_variable in date_variables:
        try:
            data_df[date_variable] = pd.to_datetime(
                data_df[date_variable], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        except Exception as e:
            error_message = f"date cast failed for column ({date_variable}): {str(e)}"
            if error_message not in error_cache:
                logger.error(error_message)
                error_cache.add(error_message)

    # Vectorized datetime conversion
    for datetime_variable in datetime_variables:
        try:
            data_df[datetime_variable] = pd.to_datetime(
                data_df[datetime_variable], errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M")
        except Exception as e:
            error_message = (
                f"datetime cast failed for column ({datetime_variable}): {str(e)}"
            )
            if error_message not in error_cache:
                logger.error(error_message)
                error_cache.add(error_message)

    # Vectorized time conversion
    for time_variable in time_variables:
        try:
            data_df[time_variable] = pd.to_datetime(
                data_df[time_variable], errors="coerce"
            ).dt.strftime("%H:%M")
        except Exception as e:
            error_message = f"time cast failed for column ({time_variable}): {str(e)}"
            if error_message not in error_cache:
                logger.error(error_message)
                error_cache.add(error_message)

    return data_df


def combine_data_from_formsdb(
    config_file: Path,
    network: str,
    event_name: str,
    df: pd.DataFrame,
    additional_dfs: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Combine the data from the formsdb database.

    Args:
        config_file (Path): Path to the config file.
        network (str): Network name.
        event_name (str): Event name.
        df (pd.DataFrame): Dataframe to export.
        additional_dfs (Dict[str, pd.DataFrame]): Pre-fetched additional column tables.

    Returns:
        pd.DataFrame: Dataframe with additional columns added.
    """
    df = add_additional_cols_fast(df=df, additional_dfs=additional_dfs)

    # Cast date columns to string
    df = cast_dates_to_str(data_df=df, config_file=config_file, network=network)

    # Drop columns with all NaN values in data columns BEFORE string conversion
    mandatory_cols = {
        "subject_id",
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
        "subjectid",
    }

    data_cols = [c for c in df.columns if c not in mandatory_cols]
    df.dropna(axis=0, how="all", subset=data_cols, inplace=True)
    df.dropna(axis=1, how="all", inplace=True)

    # Consolidated string operations - convert to string once and do all replacements
    df = df.astype(str)

    # Chain all replacements together for efficiency
    replacements = {"NaT": "", "None": "", "True": "1", "False": "0"}
    df = df.replace(replacements)

    # Remove trailing .0 patterns
    df = df.replace(r"\.0+$", "", regex=True)

    return df


def get_visit_df_bulk(
    config_file: Path, network: str, event_name: str, progress: Progress
) -> pd.DataFrame:
    """
    Fetches ALL form data for a visit in a SINGLE bulk query, then pivots in-memory.
    Replaces the old per-subject query approach (~3000 queries -> 1 query per visit).

    Args:
        config_file (Path): Path to the config file.
        network (str): Network name.
        event_name (str): Name of the event.
        progress (Progress): Rich progress bar.

    Returns:
        pd.DataFrame: Data for the event.
    """
    task = progress.add_task(f"Querying all data for {event_name}...", total=None)

    # Single bulk query: fetch all subjects' form_data for this visit + network
    query = f"""
    SELECT
        f.subject_id,
        f.event_name,
        f.form_data
    FROM
        forms.forms f
    INNER JOIN
        subjects s ON f.subject_id = s.id
    INNER JOIN
        site ON s.site_id = site.id
    WHERE
        f.event_name LIKE '%%{event_name}_arm%%'
        AND site.network_id = '{network}'
    """

    logger.info(f"Fetching bulk form data for visit={event_name}, network={network}...")
    all_forms_df = execute_sql_fast(config_file=config_file, query=query)
    progress.remove_task(task)

    if all_forms_df.empty:
        logger.warning(f"No data found for visit {event_name}")
        return pd.DataFrame()

    task = progress.add_task(
        f"Processing {len(all_forms_df)} form rows for {event_name}...", total=None
    )

    # Explode the JSON form_data column into separate columns
    all_forms_df = utils.explode_col(df=all_forms_df, col="form_data")

    # For each subject, merge rows using groupby + first (keeps first non-null per column)
    # This replaces the per-subject iterrows() merge loop
    visit_df = all_forms_df.groupby("subject_id", as_index=False).first()

    # Drop the event_name column (not needed in output)
    if "event_name" in visit_df.columns:
        visit_df.drop(columns=["event_name"], inplace=True)

    # Handle 'None' string values -> 'NoneRaw' (matches original behavior)
    visit_df = visit_df.replace({"None": "NoneRaw"})

    progress.remove_task(task)

    # Add fasting time and vial counts for baseline/month_2
    fluid_collection_timepoints = ["baseline", "month_2"]
    if event_name in fluid_collection_timepoints:
        task = progress.add_task(
            f"Fetching fasting/vial data for {event_name}...", total=None
        )

        # Bulk fetch fasting times (1 query instead of ~3000)
        fasting_query = f"""
        SELECT subject_id, time_fasting
        FROM forms_derived.subject_time_fasting
        WHERE event_name = '{event_name}'
        """
        fasting_df = execute_sql_fast(config_file=config_file, query=fasting_query)
        if not fasting_df.empty:
            visit_df = visit_df.merge(fasting_df, on="subject_id", how="left")

        # Bulk fetch vial counts (1 query instead of ~6000)
        vial_query = f"""
        SELECT subject_id, blood_vial_count, saliva_vial_count
        FROM forms_derived.subject_vials_count
        WHERE timepoint = '{event_name}'
        """
        vial_df = execute_sql_fast(config_file=config_file, query=vial_query)
        if not vial_df.empty:
            visit_df = visit_df.merge(vial_df, on="subject_id", how="left")
            # Fill missing vial counts with 0 (matches original behavior)
            for col in ["blood_vial_count", "saliva_vial_count"]:
                if col in visit_df.columns:
                    visit_df[col] = visit_df[col].fillna(0).astype(int)

        progress.remove_task(task)

    logger.info(
        f"Visit {event_name}: {len(visit_df)} subjects, {len(visit_df.columns)} columns"
    )

    return visit_df


def handle_raw_nones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles 'NoneRaw' values in the dataframe.

    Args:
        df (pd.DataFrame): Dataframe to handle 'NoneRaw' values in.

    Returns:
        pd.DataFrame: Dataframe with 'NoneRaw' values handled.
    """
    df = df.replace("NoneRaw", "None")
    return df


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

    # Pre-fetch ALL additional column tables ONCE (instead of per-visit)
    logger.info("Pre-fetching additional column tables...")
    additional_dfs = fetch_additional_cols_bulk(config_file=config_file)
    logger.info("Additional column tables fetched.")

    # Pre-warm the data dictionary cache
    data_dictionary_cache = data.get_data_dictionary(config_file=config_file)
    logger.info("Data dictionary cached.")

    with utils.get_progress_bar() as progress:
        task_networks = progress.add_task("Processing networks...", total=len(networks))
        for network in networks:
            progress.update(
                task_networks, description=f"Processing network {network}..."
            )
            task_visits = progress.add_task("Processing visits...", total=len(visits))
            for visit in visits:
                progress.update(task_visits, description=f"Processing visit {visit}...")
                visit_df = get_visit_df_bulk(
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
                    additional_dfs=additional_dfs,
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

                output_path = output_dir / f"{output_name}_v2.csv"
                visit_df.to_csv(output_path, index=False)
                logger.debug(f"Data exported to: {output_path}")

                progress.remove_task(export_task)
                progress.update(task_visits, advance=1)
            progress.update(task_networks, advance=1)

    # Clean up the shared engine
    if _engine is not None:
        _engine.dispose()

    logger.info("Done.")
