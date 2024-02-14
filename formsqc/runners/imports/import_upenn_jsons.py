#!/usr/bin/env python
"""
Import UPENN forms data from JSON to MongoDB.
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

import json
import logging
from datetime import datetime
from glob import glob
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from rich.logging import RichHandler

from formsqc import constants, data
from formsqc.helpers import db, utils

MODULE_NAME = "formsqc.runners.imports.import_upenn_jsons"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


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
        date_dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")

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
) -> Optional[str]:
    """
    Returns the closest visit name to the provided date.

    Args:
        visit_date_map (Dict[str, datetime]): A dictionary mapping visit names to visit dates.
        date (datetime): The date to find the closest visit to.

    Returns:
        Optional[str]: The closest visit name to the provided date, or
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

    return closest_visit


def generate_upenn_form(
    df_upenn: pd.DataFrame, all_forms_df: pd.DataFrame
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
        session_date = row["session_date"]
        session_id = row["session_battery"]

        session_date_dt = datetime.strptime(session_date, "%Y-%m-%d")

        if "NOSPLLT" in session_id:
            session_id = "NOSPLLT"
        else:
            session_id = "SPLLT"

        subject_id = row["session_subid"]

        try:
            visit = get_event_name_cognitive(
                all_forms_df=all_forms_df, date=session_date_dt
            )
        except ValueError:
            logger.warning(f"Falling back to visit date map for {subject_id}")
            visit_date_map = compute_visit_date_map(
                all_forms_df=all_forms_df, visits=constants.upenn_visit_order
            )

            visit = get_closest_visit(
                visit_date_map=visit_date_map, date=pd.to_datetime(session_date)
            )

        if visit is None:
            logger.warning(f"Could not find visit for {subject_id} ({session_id})")
            logger.warning(f"Falling back to visit date map for {subject_id}")

            visit_date_map = compute_visit_date_map(
                all_forms_df=all_forms_df, visits=constants.upenn_visit_order
            )

            visit = get_closest_visit(
                visit_date_map=visit_date_map, date=pd.to_datetime(session_date)
            )

            if visit is None:
                logger.warning(
                    f"Could not find visit for {subject_id}. Falling back to uncategorized"
                )
                visit = "uncategorized"
            else:
                visit = f"uncategorized ({visit})"

        if visit not in form_data:
            form_data[visit] = {}

        session_data: Dict[str, Any] = {}
        for variable in row.index:
            value = row[variable]
            if pd.isna(value):
                continue

            value = utils.str_to_typed(value)  # type: ignore
            session_data[variable] = value

        form_data[visit][session_id] = session_data
    return form_data


def upsert_form_data(
    config_file: Path, subject_id: str, form_data: Dict[str, Any]
) -> None:
    """
    Update or insert form data into the MongoDB's 'upenn' collection.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        form_data (Dict[str, Any]): The form data.

    Returns:
        None
    """
    mongodb = db.get_mongo_db(config_file)
    subject_form_data = mongodb["upenn"]

    subject_form_data.replace_one({"_id": subject_id}, form_data, upsert=True)


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
    subjects_glob = glob(
        f"{data_root}/{network}/PHOENIX/PROTECTED/*/raw/*/surveys/*.UPENN.json"
    )
    logger.info(f"Found {len(subjects_glob)} subjects for {network}")

    # Sort subjects by subject ID
    subjects_glob = sorted(subjects_glob)

    skip_buffer = []

    with utils.get_progress_bar() as progress:
        subject_process = progress.add_task(
            "[red]Processing subjects...", total=len(subjects_glob)
        )
        for subject in subjects_glob:
            subject_id = subject.split("/")[-1].split(".")[0]
            progress.update(
                subject_process,
                advance=1,
                description=f"Processing UPENN JSON for subject ({subject_id})...",
            )
            source_m_date = utils.get_file_mtime(Path(subject))

            if (
                db.check_if_subject_upenn_data_exists(
                    config_file, subject_id, source_m_date
                )
                and not force_import
            ):
                skip_buffer.append(subject_id)
                continue
            else:
                if len(skip_buffer) > 0:
                    temp_str = ", ".join(skip_buffer)
                    logger.info(
                        f"Skipping {temp_str} as data already exists, and is up to date"
                    )
                    skip_buffer = []

            with open(subject, "r", encoding="utf-8") as f:
                json_data = json.load(f)

            sub_data_all = pd.DataFrame.from_dict(json_data, orient="columns")
            sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace(
                "", np.nan
            )
            sub_data_all.dropna(axis=1, how="all", inplace=True)

            all_forms_df = data.get_all_subject_forms(
                config_file=config_file, subject_id=subject_id
            )

            form_data: Dict[str, Any] = generate_upenn_form(
                df_upenn=sub_data_all, all_forms_df=all_forms_df
            )

            # Label form data with subject ID
            form_data["_id"] = subject_id
            form_data["_date_imported"] = utils.get_curent_datetime()
            form_data["_source"] = subject
            form_data["_source_mdate"] = source_m_date

            try:
                upsert_form_data(config_file, subject_id, form_data)
            except Exception as e:
                logger.error(f"Error: {e}")
                logger.error(f"Subject: {subject_id}")
                f_name = f"{subject_id}_UPENN_DEBUG.json"
                with open(f_name, "w", encoding="utf-8") as f:
                    json.dump(form_data, f, indent=4, default=str)
                logger.error(f"Dumped subject data to {f_name}")
                raise e


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
        import_forms_by_network(
            config_file=config_file,
            network=network,
            data_root=data_root,
            force_import=FORCE_IMPORT,
        )

    logger.info("Done!")
