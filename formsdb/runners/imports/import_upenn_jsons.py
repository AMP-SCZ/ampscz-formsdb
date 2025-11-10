#!/usr/bin/env python
"""
Import UPENN forms data from JSON to Postgres.
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

import json
import logging
from datetime import datetime
from glob import glob
from typing import Any, Dict, List, Optional, Tuple
import multiprocessing

import numpy as np
import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.imports.import_upenn_jsons"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

pd.set_option("future.no_silent_downcasting", True)


def get_visit_date(all_forms_df: pd.DataFrame, visit_name: str) -> Optional[datetime]:
    """
    Get the date of a visit from the forms data.

    Args:
        all_forms_df (pd.DataFrame): The forms data.
        visit_name (str): The visit name.

    Returns:
        Optional[datetime]: The date of the visit, or None if the date is not found.
    """
    visit_df = all_forms_df[all_forms_df["event_name"].str.contains(f"{visit_name}_")]
    visit_df = visit_df[~visit_df["form_name"].str.contains("digital_biomarkers")]

    for _, row in visit_df.iterrows():
        form_data_r: Dict[str, Any] = row["form_data"]
        form_variables = form_data_r.keys()

        date_variables = [v for v in form_variables if "date" in v]
        found_dates: List[datetime] = []

        for date_variable in date_variables:
            date = form_data_r[date_variable]

            # Validate date
            if not utils.validate_date(date):
                continue
            else:
                date = pd.to_datetime(date)
                if date < datetime(2019, 1, 1):
                    continue
                found_dates.append(date.date())

        if len(found_dates) > 0:
            # return most common date
            return max(set(found_dates), key=found_dates.count)

    return None


def get_event_name_cognitive(
    all_forms_df: pd.DataFrame, date: datetime
) -> Optional[str]:
    """
    Get the event name for a cognitive visit from the forms data.

    Args:
        all_forms_df (pd.DataFrame): The forms data.
        date (datetime): The date of the visit.

    Returns:
        Optional[str]: The event name of the visit, or None if the event name is not found.
    """
    penncnb_dfs = all_forms_df[all_forms_df["form_name"].str.contains("penncnb")]

    for _, row in penncnb_dfs.iterrows():
        form_data_r: Dict[str, Any] = row["form_data"]
        form_variables = form_data_r.keys()

        if "chrpenn_interview_date" not in form_variables:
            logger.warning(
                f"chrpenn_interview_date not found in form variables for {row['event_name']}"
            )
            raise ValueError("chrpenn_interview_date not found in form variables")

        date_str = form_data_r["chrpenn_interview_date"]
        try:
            date_dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        except TypeError as exc:
            raise ValueError(
                "chrpenn_interview_date not found in form variables"
            ) from exc

        if date_dt.date() == date.date():
            return row["event_name"]

    return None


def compute_visit_date_map(
    all_forms_df: pd.DataFrame, visits: List[str]
) -> Dict[str, datetime]:
    """
    Generate a map of visit names to visit dates.

    Args:
        all_forms_df (pd.DataFrame): The forms data.
        visits (List[str]): A list of visit names.

    Returns:
        Dict[str, datetime]: A dictionary mapping visit names to visit dates.
    """
    visit_date_map: Dict[str, datetime] = {}

    for visit_name in visits:
        visit_date = get_visit_date(all_forms_df=all_forms_df, visit_name=visit_name)

        if visit_date is not None:
            visit_date_map[visit_name] = visit_date

    return visit_date_map


def get_closest_visit(
    visit_date_map: Dict[str, datetime], date: datetime
) -> Tuple[Optional[str], Optional[datetime]]:
    """
    Returns the closest visit name to the provided date.

    Args:
        visit_date_map (Dict[str, datetime]): A dictionary mapping visit names to visit dates.
        date (datetime): The date to find the closest visit to.

    Returns:
        Tuple[Optional[str], datetime]: The closest visit name and date.
            None if no visit is close enough.
    """
    closest_visit: Optional[str] = None
    closest_visit_diff: Optional[int] = None

    for visit_name, visit_date in visit_date_map.items():
        # Convert visit_date to datetime if necessary
        if not isinstance(visit_date, datetime):
            visit_date = pd.to_datetime(visit_date)

        diff = abs((date - visit_date).days)

        if closest_visit_diff is None or diff < closest_visit_diff:
            closest_visit = visit_name
            closest_visit_diff = diff

    closest_visit_date = visit_date_map.get(closest_visit, None)  # type: ignore
    return closest_visit, closest_visit_date  # type: ignore


def merge_dicts(d1: Dict[str, Any], d2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two dictionaries.

    Results in a dictionary like {
        key1: value1, # if value1 == value2
        key2: [value2, value3], # if value2 != value3
    }

    Args:
        d1 (Dict[str, Any]): The first dictionary.
        d2 (Dict[str, Any]): The second dictionary.

    Returns:
        Dict[str, Any]: The merged dictionary.
    """
    merged_dict = {}

    for key, value in d1.items():
        merged_dict[key] = value

    for key, value in d2.items():
        if key in merged_dict:
            if merged_dict[key] != value:
                if isinstance(merged_dict[key], list):
                    merged_dict[key].append(value)
                else:
                    merged_dict[key] = [merged_dict[key], value]
        else:
            merged_dict[key] = value

    return merged_dict


def generate_upenn_form(
    subject_id: str,
    df_upenn: pd.DataFrame,
    all_forms_df: pd.DataFrame,
    config_file: Path,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Break down the UPENN form data into a dictionary of visit names, session IDs, and session data.

    Potential session IDs are "SPLLT" and "NOSPLLT".

    Args:
        df_upenn (pd.DataFrame): The UPENN form data.
        all_forms_df (pd.DataFrame): The forms data.

    Returns:
        Dict[str, Dict[str, Dict[str, Any]]]: A dictionary of visit names, session IDs,
            and session data.
    """
    form_data: Dict[str, Dict[str, Any]] = {}

    for _, row in df_upenn.iterrows():
        session_date = row["interview_date"]  # 2023-06-19
        try:
            session_date_dt = datetime.strptime(session_date, "%Y-%m-%d")
        except ValueError:
            logger.warning(
                f"Could not parse date interview_date: {session_date} for {subject_id}. Skipping."
            )
            logger.warning("Expected format: m/d/y")
            raise

        try:
            visit = get_event_name_cognitive(
                all_forms_df=all_forms_df, date=session_date_dt
            )
            visit_date = session_date_dt
        except ValueError:
            logger.warning(f"Falling back to visit date map for {subject_id}")
            visit_date_map = compute_visit_date_map(
                all_forms_df=all_forms_df, visits=constants.upenn_visit_order
            )

            visit, visit_date = get_closest_visit(
                visit_date_map=visit_date_map, date=pd.to_datetime(session_date)
            )

        if visit is None:
            logger.warning(
                f"Could not find visit for {subject_id}, falling back to visit date map"
            )

            visit_date_map = compute_visit_date_map(
                all_forms_df=all_forms_df, visits=constants.upenn_visit_order
            )

            visit, visit_date = get_closest_visit(
                visit_date_map=visit_date_map, date=pd.to_datetime(session_date)
            )

            if visit is None:
                logger.warning(
                    f"Could not find visit for {subject_id}. Falling back to uncategorized"
                )
                visit = "uncategorized"
            else:
                cohort = data.get_subject_cohort(
                    config_file=config_file, subject_id=subject_id
                )
                match cohort:
                    case "CHR":
                        visit = f"{visit}_arm_1"
                    case "HC":
                        visit = f"{visit}_arm_2"
                    case _:
                        visit = f"{visit}_arm_x"

        session_data: Dict[str, Any] = {}
        for variable in row.index:
            value = row[variable]
            if pd.isna(value):  # type: ignore
                continue

            value = utils.str_to_typed(value)  # type: ignore
            session_data[variable] = value

        if visit_date is not None:
            session_data["penncnb_interview_date"] = visit_date.strftime("%Y-%m-%d")

        if visit not in form_data:
            form_data[visit] = session_data
        else:
            form_data[visit] = merge_dicts(form_data[visit], session_data)
    return form_data


def construct_insert_queries(
    subject_id: str,
    form_data: Dict[str, Any],
) -> List[str]:
    """
    Update or insert form data into the Postgres database forms.upenn_forms table.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        form_data (Dict[str, Any]): The form data.

    Returns:
        List[str]: A list of SQL queries to update subject data.
    """
    sql_queries: List[str] = []

    delete_query = f"DELETE FROM forms.upenn_forms WHERE subject_id = '{subject_id}';"
    sql_queries.append(delete_query)

    form_metadata = form_data.pop("metadata")
    source_m_date = form_metadata["source_m_date"]
    for event_name, event_form_data in form_data.items():
        sql_query = f"""
        INSERT INTO forms.upenn_forms (subject_id, event_name, event_type,
            form_data, source_mdate)
        VALUES ('{subject_id}', '{event_name}', 'nda',
            '{db.sanitize_json(event_form_data)}', '{source_m_date}');
        """
        sql_queries.append(sql_query)

    return sql_queries


def process_subject(params: Tuple[Path, str, bool]) -> Tuple[str, bool, List[str]]:
    """
    A function to process a subject's UPENN JSON file.

    Args:
        params (Tuple[Path, Path, str, bool]): A tuple containing the config file,
            subject JSON file, the source, and whether to force import the data.

    Returns:
        Tuple[str, bool, List[str]]: A tuple containing the subject ID, whether the subject was
            imported successfully, and a list of SQL queries.
    """
    config_file, subject_json, force_import = params

    subject_id = subject_json.split("/")[-1].split(".")[0]
    source_m_date = utils.get_file_mtime(Path(subject_json))
    if (
        db.check_if_subject_upenn_data_exists(
            config_file,
            subject_id,
            source_m_date,
        )
        and not force_import
    ):
        return subject_id, False, []

    with open(subject_json, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    sub_data_all = pd.DataFrame.from_dict(json_data, orient="columns")
    sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace("", np.nan)
    sub_data_all.dropna(axis=1, how="all", inplace=True)

    all_forms_df = data.get_all_subject_forms(
        config_file=config_file, subject_id=subject_id
    )

    # try:
    form_data: Dict[str, Any] = generate_upenn_form(
        subject_id=subject_id,
        df_upenn=sub_data_all,
        all_forms_df=all_forms_df,
        config_file=config_file,
    )
    # except Exception as e:
    #     logger.error(f"Error: {e}")
    #     logger.error(f"JSON file: {subject_json}")

    # Label form data with subject ID
    form_metadata: Dict[str, Any] = {
        "subject_id": subject_id,
        "source": subject_json,
        "source_m_date": source_m_date,
    }
    form_data["metadata"] = form_metadata

    try:
        sql_queries = construct_insert_queries(
            subject_id=subject_id, form_data=form_data
        )
        return subject_id, True, sql_queries
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(f"Subject: {subject_id}")
        f_name = f"{subject_id}_UPENN_DEBUG.json"
        with open(f_name, "w", encoding="utf-8") as f:
            json.dump(form_data, f, indent=4, default=str)
        logger.error(f"Dumped subject data to {f_name}")
        raise e


def import_forms_by_network(
    config_file: Path, network: str, data_root: Path, force_import: bool = False
) -> None:
    """
    Import UPENN forms data for a network.

    Args:
        config_file (Path): The path to the configuration file.
        network (str): The network name.
        data_root (Path): The root path of the data.
        force_import (bool): Whether to force import the data.

    Returns:
        None
    """
    skipped_count = 0
    imported_count = 0

    search_pattern = (
        f"{data_root}/{network}/PHOENIX/PROTECTED/*/raw/*/surveys/*.UPENN_nda.json"
    )
    logger.info(f"Searching for subjects in {search_pattern}")
    subjects_glob = glob(search_pattern)
    logger.info(f"Found {len(subjects_glob)} subjects for {network}")

    # Sort subjects by subject ID
    subjects_glob = sorted(subjects_glob)

    num_processes = 16
    logger.info(f"Using {num_processes} processes.")

    log_frequency = max(1, len(subjects_glob) // 10)

    skip_buffer = []
    params = [
        (config_file, subject_json, force_import) for subject_json in subjects_glob
    ]

    sql_queries: List[str] = []
    with utils.get_progress_bar() as progress:
        with multiprocessing.Pool(processes=int(num_processes)) as pool:
            task = progress.add_task(f"Processing {network}...", total=len(params))
            completed_processing: int = 0
            for result in pool.imap_unordered(process_subject, params):
                subject_id, imported, subject_sql_queries = result
                if imported:
                    imported_count += 1
                else:
                    skipped_count += 1
                    skip_buffer.append(subject_id)
                completed_processing += 1

                if completed_processing % log_frequency == 0:
                    logger.info(
                        f"Processed {completed_processing}/{len(params)} subjects "
                        f"for {network} - Imported: {imported_count}, Skipped: {skipped_count}"
                    )

                sql_queries.extend(subject_sql_queries)
                progress.update(task, advance=1)

        logger.info(f"Skipped {skipped_count} subjects")
        logger.info(f"Imported {imported_count} subjects")

        if imported_count < 1:
            logger.warning(f"No subjects imported for {network}")

    if len(sql_queries) > 0:
        db.execute_queries(
            config_file=config_file,
            queries=sql_queries,
            show_commands=False,
            show_progress=True,
        )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    data_params = utils.config(config_file, "data")
    data_root = Path(config_params["data_root"])

    FORCE_IMPORT = False
    logger.info(f"Force import: {FORCE_IMPORT}")

    for network in constants.networks:
        logger.info(f"Importing UPENN data for {network}")
        import_forms_by_network(
            config_file=config_file,
            network=network,
            data_root=data_root,
            force_import=FORCE_IMPORT,
        )

    logger.info("Done!")
