#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "ampscz-formsqc":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import copy
import json
import logging
from datetime import datetime
from glob import glob
from typing import Any, Dict, Tuple, List
import re

import numpy as np
import pandas as pd
from rich.logging import RichHandler
from rich.progress import Progress

from formsqc.helpers import db, utils, hash
from formsqc import constants

MODULE_NAME = "formsqc_json_importer"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def generate_all_forms(
    df_all_forms: pd.DataFrame, data_dictionry: pd.DataFrame, progress: Progress
) -> Dict[str, Dict[str, Dict[str, Any]]]:
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
                    skip_cast_pattern = r"barcode$|box|_id$|\d+id$|_id\d."
                    # if (
                    #     variable.endswith("barcode")
                    #     or "_box" in variable
                    #     or "_id" in variable
                    #     # endswith "X_id" where X is a number
                    #     or (variable.endswith("_id") and variable[-4].isdigit())
                    # ):
                    if re.search(skip_cast_pattern, variable):
                        # logger.warning(
                        #     f"Value {value} for [{form_name}]:{variable} is too large for MongoDB"
                        # )
                        # logger.warning(f"Casting {variable} to string")
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


def upset_form_data(
    config_file: Path, subject_id: str, form_data: Dict[str, Any]
) -> None:
    mongodb = db.get_mongo_db(config_file)
    subject_form_data = mongodb["forms"]

    subject_form_data.update_one(
        {"_id": subject_id},
        {"$set": form_data},
        upsert=True,
    )


def import_forms_by_network(
    config_file: Path, network: str, data_root: Path, data_dictionary: Path
) -> None:
    subjects_glob = glob(
        f"{data_root}/{network}/PHOENIX/PROTECTED/*/raw/*/surveys/*.{network}.json"
    )
    subjects_glob = sorted(subjects_glob, reverse=True)
    logger.info(f"Found {len(subjects_glob)} subjects for {network}")

    data_dictionry_df = pd.read_csv(
        data_dictionary, sep=",", index_col=False, low_memory=True
    )

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
            source_hash = hash.compute_hash(Path(subject))

            if db.check_if_subject_form_data_exists(
                config_file, subject_id, source_hash
            ):
                skip_buffer.append(subject_id)
                continue
            else:
                skip_buffer = _empty_buffer(skip_buffer)
                processed_buffer.append(subject_id)

            with open(subject, "r") as f:
                json_data = json.load(f)

            sub_data_all = pd.DataFrame.from_dict(json_data, orient="columns")
            sub_data_all = sub_data_all.apply(lambda x: x.str.strip()).replace(
                "", np.nan
            )
            sub_data_all.dropna(axis=1, how="all", inplace=True)

            form_data: Dict[str, Any] = generate_all_forms(
                sub_data_all, data_dictionry_df, progress
            )

            # Append form statistics
            for form_name in form_data.keys():
                for event in form_data[form_name].keys():
                    form_data[form_name][event] = append_append_form_statistics(
                        form_data[form_name][event], form_name, data_dictionry_df
                    )

            # Label form data with subject ID
            form_data["_id"] = subject_id
            form_data["_date_imported"] = utils.get_curent_datetime()
            form_data["_source"] = subject
            form_data["_source_md5"] = hash.compute_hash(Path(subject))
            form_data["_source_mdate"] = source_m_date

            try:
                upset_form_data(config_file, subject_id, form_data)
            except Exception as e:
                logger.error(f"Error: {e}")
                logger.error(f"Subject: {subject_id}")
                with open(f"{subject_id}_DEBUG.json", "w") as f:
                    json.dump(form_data, f, indent=4, default=str)
                logger.error(f"Dumped subject data to {subject_id}_DEBUG.json")
                raise e

        _empty_buffer(skip_buffer)

    logger.info(f"Processed {len(processed_buffer)} subjects")
    if len(processed_buffer) > 0:
        logger.info(f"Processed subjects: {', '.join(processed_buffer)}")


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

    for network in constants.networks:
        logger.info(f"Importing {network} data...")
        import_forms_by_network(
            config_file=config_file,
            network=network,
            data_root=data_root,
            data_dictionary=data_dictionary_f,
        )

    logger.info("Done!")
