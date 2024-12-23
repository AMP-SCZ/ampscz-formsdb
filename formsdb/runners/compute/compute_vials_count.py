#!/usr/bin/env python
"""
Counts the number of Blood and Saliva vials
collected for each participant.
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
from typing import Any, Dict, List, Tuple

import pandas as pd
from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.compute.compute_vials_count"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def count_subject_blood_vials(
    config_file: Path, subject_id: str, event_name: str
) -> int:
    """
    Count the number of blood vials collected for a subject at a given event.

    Args:
        config_file (Path): Path to the configuration file.
        subject_id (str): Subject ID.
        event_name (str): Event name.

    Returns:
        int: Number of blood vials collected.
    """

    vial_variables: List[str] = [
        "chrblood_bc1vol",
        "chrblood_wb1vol",
        "chrblood_wb2vol",
        "chrblood_wb3vol",
        "chrblood_se1vol",
        "chrblood_se2vol",
        "chrblood_se3vol",
        "chrblood_pl1vol",
        "chrblood_pl2vol",
        "chrblood_pl3vol",
        "chrblood_pl4vol",
        "chrblood_pl5vol",
        "chrblood_pl6vol",
    ]

    vial_count = 0

    for variable in vial_variables:
        vial_volume = data.get_variable(
            config_file=config_file,
            subject_id=subject_id,
            form_name="blood_sample_preanalytic_quality_assurance",
            event_name=event_name,
            variable_name=variable,
        )

        if vial_volume is not None:
            vial_volume = float(vial_volume)
            if vial_volume > 0:
                vial_count += 1

    return vial_count


def count_subject_saliva_vials(
    config_file: Path, subject_id: str, event_name: str
) -> int:
    """
    Count the number of saliva vials collected for a subject at a given event.

    Args:
        config_file (Path): Path to the configuration file.
        subject_id (str): Subject ID.
        event_name (str): Event name.

    Returns:
        int: Number of saliva vials collected for the subject at the event.
    """

    vial_variables: List[str] = [
        "chrsaliva_vol1a",
        "chrsaliva_vol1b",
        "chrsaliva_vol2a",
        "chrsaliva_vol2b",
        "chrsaliva_vol3a",
        "chrsaliva_vol3b",
    ]

    vial_count = 0

    for variable in vial_variables:
        vial_volume = data.get_variable(
            config_file=config_file,
            subject_id=subject_id,
            form_name="daily_activity_and_saliva_sample_collection",
            event_name=event_name,
            variable_name=variable,
        )

        if vial_volume is not None:
            vial_volume = float(vial_volume)
            if vial_volume > 0:
                vial_count += 1

    return vial_count


def process_data(params: Tuple[Path, str, str]) -> Dict[str, Any]:
    """
    Returns the number of blood and saliva vials collected for a subject at a given event.

    Args:
        params (Tuple[Path, str, str]): Tuple containing the configuration file path,
            subject ID, and event name.

    Returns:
        Dict[str, Any]: Dictionary containing the subject ID, event name,
            number of saliva vials, and number of blood vials collected.
    """
    config_file, subject_id, timepoint = params

    blood_vial_count = count_subject_blood_vials(
        config_file=config_file, subject_id=subject_id, event_name=timepoint
    )

    saliva_vial_count = count_subject_saliva_vials(
        config_file=config_file, subject_id=subject_id, event_name=timepoint
    )

    result = {
        "subject_id": subject_id,
        "timepoint": timepoint,
        "saliva_vial_count": saliva_vial_count,
        "blood_vial_count": blood_vial_count,
    }

    return result


def count_vials(config_file: Path) -> pd.DataFrame:
    """
    Returns a DataFrame with the number of blood and saliva vials collected for each subject.

    Args:
        config_file (Path): Path to the configuration file.

    Returns:
        pd.DataFrame: DataFrame containing the subject ID, event name,
            number of saliva vials, and number of blood vials collected.
    """
    subject_ids = data.get_all_subjects(config_file=config_file)
    timepoints: List[str] = ["baseline", "month_2"]

    params = [
        (config_file, subject_id, timepoint)
        for subject_id in subject_ids
        for timepoint in timepoints
    ]
    results: List[Dict[str, Any]] = []

    with multiprocessing.Pool() as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task(
                "Counting blood and saliva vials", total=len(params)
            )
            for result in pool.imap_unordered(process_data, params):  # type: ignore
                if result:
                    results.append(result)
                progress.update(task, advance=1)

    results_df = pd.DataFrame(results)

    # Drop rows with 0 in the blood_vial_count and saliva_vial_count columns
    results_df = results_df[
        (results_df["blood_vial_count"] > 0) & (results_df["saliva_vial_count"] > 0)
    ]

    return results_df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    logger.info(
        "Counting the number of blood and saliva vials collected for each subject."
    )
    vials_count_df = count_vials(config_file=config_file)
    db.df_to_table(
        df=vials_count_df,
        schema="forms_derived",
        table_name="subject_vials_count",
        config_file=config_file,
        if_exists="replace",
    )

    logger.info("Done.")
