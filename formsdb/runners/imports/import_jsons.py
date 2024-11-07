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
import re
from datetime import datetime
from glob import glob
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from rich.logging import RichHandler
from rich.progress import Progress

from formsdb import constants
from formsdb.helpers import db, utils
from formsdb.helpers import hash as hash_helper

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
    df_all_forms: pd.DataFrame, data_dictionry: pd.DataFrame, progress: Progress
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
    variable_progress = progress.add_task(
        f"Importing Variables ({len(df_cols)})", total=len(df_cols)
    )
    form_name = None
    for variable in df_cols:
        progress.update(variable_progress, advance=1)
        if variable == "redcap_event_name":
            continue
        try:
            form_name = data_dictionry.loc[
                data_dictionry["Variable / Field Name"] == variable
            ]["Form Name"].values[0]
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

    # Remove progress bar
    progress.remove_task(variable_progress)

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
    form_dict = data_dictionry.loc[data_dictionry["Form Name"] == form_name]
    form_vars = form_dict["Variable / Field Name"].unique().tolist()

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


def import_forms_by_network(
    config_file: Path,
    network: str,
    data_root: Path,
    data_dictionary: Path,
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
    global FAILED_IMPORT_SUBJECTS

    subjects_glob = glob(
        f"{data_root}/{network}/PHOENIX/PROTECTED/*/raw/*/surveys/*.{network}.json"
    )
    subjects_glob = sorted(subjects_glob, reverse=False)
    logger.info(f"Found {len(subjects_glob)} subjects for {network}")

    data_dictionry_df = pd.read_csv(data_dictionary, sep=",")

    skip_buffer: List[str] = []
    processed_buffer: List[str] = []

    def _empty_buffer(skip_buffer) -> List[str]:
        if len(skip_buffer) > 0:
            temp_str = ", ".join(skip_buffer)
            logger.info(
                f"Skipping {temp_str} as form data already exists, and is up to date"
            )
            skip_buffer = []
        return skip_buffer

    with utils.get_progress_bar() as progress:
        subject_process = progress.add_task(
            "[red]Processing subjects...", total=len(subjects_glob)
        )
        for subject in subjects_glob:
            subject_id = subject.split("/")[-1].split(".")[0]
            progress.update(
                subject_process,
                advance=1,
                description=f"Processing JSON for subject ({subject_id})...",
            )
            source_m_date = utils.get_file_mtime(Path(subject))
            source_hash = hash_helper.compute_hash(Path(subject))

            if not force:
                up_to_date = db.check_if_subject_form_data_exists(
                    config_file, subject_id, source_hash
                )

                if up_to_date:
                    skip_buffer.append(subject_id)
                    continue
                else:
                    skip_buffer = _empty_buffer(skip_buffer)
            processed_buffer.append(subject_id)

            try:
                with open(subject, "r", encoding="utf-8") as f:
                    json_data = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Error: {e}")
                logger.error(f"JSON file: {subject}")
                logger.error(f"Import failed for {subject_id}")
                FAILED_IMPORT_SUBJECTS.append(subject_id)
                continue

            sub_data_all = pd.DataFrame.from_dict(json_data, orient="columns")
            sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace(
                "", np.nan
            )
            sub_data_all.dropna(axis=1, how="all", inplace=True)

            form_data: Dict[str, Any] = generate_all_forms(
                sub_data_all, data_dictionry_df, progress
            )

            # Append form statistics
            for form_name, _ in form_data.items():
                for event, _ in form_data[form_name].items():
                    form_data[form_name][event] = append_append_form_statistics(
                        form_data[form_name][event], form_name, data_dictionry_df
                    )

            # Label form data with subject ID
            form_data["_id"] = subject_id
            form_data["_date_imported"] = utils.get_curent_datetime()
            form_data["_source"] = subject
            form_data["_source_md5"] = hash_helper.compute_hash(Path(subject))
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

        _empty_buffer(skip_buffer)

    logger.info(f"Processed {len(processed_buffer)} subjects")
    if len(processed_buffer) > 0:
        logger.info(f"Processed subjects: {', '.join(processed_buffer)}")
    if len(FAILED_IMPORT_SUBJECTS) > 0:
        logger.error(f"Failed to import {len(FAILED_IMPORT_SUBJECTS)} subjects")
        logger.error(f"Failed subjects: {', '.join(FAILED_IMPORT_SUBJECTS)}")


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    data_params = utils.config(config_file, "data")
    data_dictionary_f = Path(data_params["data_dictionary"])

    logger.info(f"Using data dictionary: {data_dictionary_f}")
    data_root = Path(config_params["data_root"])

    FORCE = False
    logger.info(f"Force: {FORCE}")

    networks = constants.networks
    # remove 'Prescient' network (use RPMS imported, not JSON)
    networks.remove("Prescient")

    for network in networks:
        logger.info(f"Importing {network} data...")
        import_forms_by_network(
            config_file=config_file,
            network=network,
            data_root=data_root,
            data_dictionary=data_dictionary_f,
            force=FORCE,
        )

    logger.info("Done!")
