#!/usr/bin/env python
"""
Import JSON data into MongoDB.

Note: Only imports JSON files with a different hash from the existing data.
    (unless force is set to True)
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

import copy
import json
import logging
import multiprocessing
import re
from datetime import datetime
from glob import glob
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import db
from formsdb.helpers import hash as hash_helper
from formsdb.helpers import utils

MODULE_NAME = "formsdb.runners.imports.import_jsons"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

FAILED_IMPORT_SUBJECTS: List[str] = []


def generate_all_forms(
    df_all_forms: pd.DataFrame, data_dictionry: pd.DataFrame
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Breaks down the dataframe into a dictionary of forms and events
    with their respective variables and values.

    Args:
        df_all_forms (pd.DataFrame): The dataframe containing all forms data.
        data_dictionry (pd.DataFrame): The data dictionary dataframe.
        progress (Progress): The rich Progress object.

    Returns:
        Dict[str, Dict[str, Dict[str, Any]]]: The form data.
    """
    form_data: Dict[str, Dict[str, Dict[str, Any]]] = {}

    df_cols = df_all_forms.columns.tolist()
    form_name = None
    for variable in df_cols:
        if variable == "redcap_event_name":
            continue
        try:
            form_name = data_dictionry.loc[
                data_dictionry["field_name"] == variable
            ]["form_name"].values[0]
        except IndexError:
            if "uncategorized" not in form_data:
                form_data["uncategorized"] = {}
            form_name = "uncategorized"

        if form_name not in form_data:
            form_data[form_name] = {}

        values: Tuple[str, str] = (
            df_all_forms[[variable, "redcap_event_name"]].dropna().values.tolist()
        )
        for value in values:
            value, event = value

            if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
                value = int(value)
                # Handle: MongoDB can only handle up to 8-byte int
                if value > 2147483647 or value < -2147483648:
                    skip_cast_pattern = r"barcode$|box|_id$|\d+id$|_id\d.|id_\d$"
                    if re.search(skip_cast_pattern, variable):
                        value = str(value)
                    else:
                        logger.warning(
                            f"Value {value} for [{form_name}]:{variable} is too large for MongoDB"
                        )
                        logger.warning(f"Casting {variable} to float")
                        value = float(value)
            elif value.replace(".", "", 1).isdigit() or (
                value.startswith("-") and value[1:].replace(".", "", 1).isdigit()
            ):
                value = float(value)
            # Check if matches date format
            elif utils.is_date(value):
                value = datetime.strptime(value, "%Y-%m-%d")
            elif utils.is_time(value):
                value = datetime.strptime(value, "%H:%M")
            elif utils.is_datetime(value):
                value = datetime.strptime(value, "%Y-%m-%d %H:%M")

            if event not in form_data[form_name]:
                form_data[form_name][event] = {}
            form_data[form_name][event][variable] = value

    return form_data


def append_append_form_statistics(
    form_data: Dict[str, str], form_name: str, data_dictionry: pd.DataFrame
) -> Dict[str, Any]:
    """
    Append form statistics to the form data. Statistics include:
    - Variables with data (count)
    - Variables without data (count)
    - Total variables (count)
    - Percent data available (percentage)

    Args:
        form_data (Dict[str, str]): The form data.
        form_name (str): The form name.
        data_dictionry (pd.DataFrame): The data dictionary dataframe.

    Returns:
        Dict[str, Any]: The form data with statistics appended.
    """
    form_dict = data_dictionry.loc[data_dictionry["form_name"] == form_name]
    form_vars = form_dict["field_name"].unique().tolist()

    result_dict: Dict[str, Any] = copy.deepcopy(form_data)

    if form_name == "uncategorized":
        result_dict["metadata"] = {
            "variables_with_data": len(form_data.keys()),
        }
        return result_dict

    variables_with_data = len(form_data.keys())
    variables_without_data = len(form_vars) - variables_with_data
    percent_data_available = variables_with_data / len(form_vars) * 100

    result_dict["metadata"] = {
        "variables_with_data": variables_with_data,
        "variables_without_data": variables_without_data,
        "total_variables": len(form_vars),
        "percent_data_available": percent_data_available,
    }

    return result_dict


def upsert_form_data(
    config_file: Path, subject_id: str, form_data: Dict[str, Any]
) -> None:
    """
    Update or insert form data into MongoDB.

    Args:
        config_file (Path): The path to the config file.
        subject_id (str): The subject ID.
        form_data (Dict[str, Any]): The form data.

    Returns:
        None
    """
    mongodb = db.get_mongo_db(config_file)
    subject_form_data = mongodb["forms"]

    subject_form_data.update_one(
        {"_id": subject_id},
        {"$set": form_data},
        upsert=True,
    )


def delete_subject_form_data(config_file: Path, subject_id: str) -> None:
    """
    Deletes all form data for a subject.

    Args:
        config_file (Path): The path to the config file.
        subject_id (str): The subject ID.

    Returns:
        None
    """
    mongodb = db.get_mongo_db(config_file)
    subject_form_data = mongodb["forms"]

    subject_form_data.delete_many({"_id": subject_id})
    return


def process_subject(
    subject_json: Path,
    data_dictionry_df: pd.DataFrame,
    config_file: Path,
    force: bool = False,
) -> Tuple[bool, str]:
    """
    Process a subject JSON file.

    Args:
        subject_json (Path): The path to the subject JSON file.
        data_dictionary (pd.DataFrame): The data dictionary dataframe.
        force (bool): Whether to force import.

    Returns:
        bool: True if successful, False otherwise.
    """
    subject_id = str(subject_json).split("/")[-1].split(".")[0]
    source_m_date = utils.get_file_mtime(subject_json)
    source_hash = hash_helper.compute_hash(subject_json)

    if not force:
        up_to_date = db.check_if_subject_form_data_exists(
            config_file, subject_id, source_hash
        )

        if up_to_date:
            return False, subject_id

    try:
        with open(subject_json, "r", encoding="utf-8") as f:
            json_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error: {e}")
        logger.error(f"JSON file: {subject_json}")
        logger.error(f"Import failed for {subject_id}")
        FAILED_IMPORT_SUBJECTS.append(subject_id)
        return False, subject_id

    sub_data_all = pd.DataFrame.from_dict(json_data, orient="columns")
    sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace("", np.nan)
    sub_data_all.dropna(axis=1, how="all", inplace=True)

    form_data: Dict[str, Any] = generate_all_forms(sub_data_all, data_dictionry_df)

    # Append form statistics
    for form_name, _ in form_data.items():
        for event, _ in form_data[form_name].items():
            form_data[form_name][event] = append_append_form_statistics(
                form_data[form_name][event], form_name, data_dictionry_df
            )

    # Label form data with subject ID
    form_data["_id"] = subject_id
    form_data["_date_imported"] = utils.get_curent_datetime()
    form_data["_source"] = str(subject_json)
    form_data["_source_md5"] = hash_helper.compute_hash(subject_json)
    form_data["_source_mdate"] = source_m_date

    try:
        delete_subject_form_data(config_file, subject_id)
        upsert_form_data(config_file, subject_id, form_data)
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(f"Subject: {subject_id}")
        with open(f"{subject_id}_DEBUG.json", "w", encoding="utf-8") as f:
            json.dump(form_data, f, indent=4, default=str)
        logger.error(f"Dumped subject data to {subject_id}_DEBUG.json")
        raise e

    return True, subject_id


def process_subject_wrapper(
    args: Tuple[Path, pd.DataFrame, Path, bool]
) -> Tuple[bool, str]:
    """
    Wrapper for process_subject to allow for multiprocessing.
    """
    return process_subject(*args)


def import_forms_by_network(
    network: str,
    data_root: Path,
    # data_dictionary: Path,
    config_file: Path,
    force: bool = False,
) -> None:
    """
    Import forms data by reading JSON files from the data root.

    Args:
        config_file (Path): The path to the config file.
        network (str): The network name.
        data_root (Path): The path to the data root.
        data_dictionary (Path): The path to the data dictionary.

    Returns:
        None
    """

    subjects_glob = glob(
        f"{data_root}/{network}/PHOENIX/PROTECTED/*/raw/*/surveys/*.{network}.json"
    )
    subjects_glob = sorted(subjects_glob, reverse=False)
    logger.info(f"Found {len(subjects_glob)} subjects for {network}")

    data_dictionry_df = data.get_data_dictionary(config_file=config_file)

    skipped_subjects: List[str] = []
    processed_subjects: List[str] = []

    num_processes = multiprocessing.cpu_count() // 4
    logger.info(f"Using {num_processes} processes.")

    params = [
        (Path(subject), data_dictionry_df, config_file, force)
        for subject in subjects_glob
    ]

    with utils.get_progress_bar() as progress:
        with multiprocessing.Pool(num_processes) as pool:
            task = progress.add_task(
                f"Processing {network} JSONs...", total=len(subjects_glob)
            )
            for result in pool.imap_unordered(process_subject_wrapper, params):
                imported, subject_id = result
                if imported:
                    processed_subjects.append(subject_id)
                else:
                    skipped_subjects.append(subject_id)
                progress.update(task, advance=1)

    logger.info(f"Processed {len(processed_subjects)} subjects")
    if len(processed_subjects) > 0:
        logger.info(f"Processed subjects: {', '.join(processed_subjects)}")
    if len(skipped_subjects) > 0:
        logger.error(f"Failed to import {len(skipped_subjects)} subjects")
        logger.error(f"Failed subjects: {', '.join(skipped_subjects)}")


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    data_root = Path(config_params["data_root"])

    FORCE = True
    logger.info(f"Force: {FORCE}")

    networks = constants.networks
    # remove 'Prescient' network (use RPMS imported, not JSON)
    networks.remove("Prescient")

    for network in networks:
        logger.info(f"Importing {network} data...")
        import_forms_by_network(
            network=network,
            data_root=data_root,
            config_file=config_file,
            force=FORCE,
        )

    logger.info("Done!")
