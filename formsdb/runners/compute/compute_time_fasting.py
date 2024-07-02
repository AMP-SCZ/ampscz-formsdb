#!/usr/bin/env python
"""
Computes the time fasting for each participant.
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
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.compute.compute_time_fasting"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_subject_fasting_timedelta(
    config_file: Path, subject_id: str, event_name: str
) -> Optional[timedelta]:
    """
    Fasting time is calculated as the difference between the blood draw date and the last time the subject ate.

    time_fasting = chrblood_drawdate - chrchs_ate
    blood_sample_preanalytic_quality_assurance -> chrblood_drawdate
    current_health_status -> chrchs_ate

    Args:
        config_file (Path): Path to the configuration file.
        subject_id (str): Subject ID.
        event_name (str): Event name.

    Returns:
        Optional[timedelta]: Fasting time.
    """

    chrblood_drawdate = data.get_variable(
        config_file=config_file,
        subject_id=subject_id,
        form_name="blood_sample_preanalytic_quality_assurance",
        event_name=event_name,
        variable_name="chrblood_drawdate",
    )

    chrchs_ate = data.get_variable(
        config_file=config_file,
        subject_id=subject_id,
        form_name="current_health_status",
        event_name=event_name,
        variable_name="chrchs_ate",
    )

    if chrblood_drawdate is None or chrchs_ate is None:
        return None

    try:
        chrblood_drawdate = pd.to_datetime(chrblood_drawdate)
        chrchs_ate = pd.to_datetime(chrchs_ate)
    except ValueError:
        return None

    time_fasting = chrblood_drawdate - chrchs_ate

    return time_fasting


def timedelta_to_hhmmss(delta: timedelta) -> str:
    """
    Convert timedelta to HH:MM:SS format.

    Args:
        delta (timedelta): Timedelta.

    Returns:
        str: HH:MM:SS format.
    """
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    str_rep = f"{hours:02}:{minutes:02}:{seconds:02}"

    return str_rep


def process_data(params: Tuple[Path, str, str]) -> Optional[Dict[str, Any]]:
    config_file, subject_id, timepoint = params
    time_fasting = get_subject_fasting_timedelta(
        config_file=config_file, subject_id=subject_id, event_name=timepoint
    )

    if time_fasting:
        time_fasting_str = timedelta_to_hhmmss(time_fasting)
        return {
            "subject_id": subject_id,
            "event_name": timepoint,
            "time_fasting": time_fasting_str,
        }

    return None


def compute_time_fasting(config_file: Path) -> pd.DataFrame:
    """
    Returns a DataFrame with the time fasting for each subject.

    Args:
        config_file (Path): Path to the configuration file.

    Returns:
        pd.DataFrame: DataFrame containing the subject ID, event name,
            time_fasting in HH:MM:SS format.
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
            task = progress.add_task("Calculating fasting time", total=len(params))
            for result in pool.imap_unordered(process_data, params):  # type: ignore
                if result:
                    results.append(result)
                progress.update(task, advance=1)

    results_df = pd.DataFrame(results)

    return results_df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    logger.info("Counting the time fasting for each subject...")
    vials_count_df = compute_time_fasting(config_file=config_file)
    db.df_to_table(
        df=vials_count_df,
        table_name="subject_time_fasting",
        config_file=config_file,
        if_exists="replace",
    )

    logger.info("Done.")
