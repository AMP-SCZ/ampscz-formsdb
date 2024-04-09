#!/usr/bin/env python
"""
Updates the blood_metrics table in the database with the computed blood metrics.
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

MODULE_NAME = "formsdb.runners.compute.compute_blood_metrics"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_subject_blood_status(
    subject_id: str, config_file: Path
) -> List[Dict[str, Any]]:
    """
    Get the flood metrics for each event for a subject.

    Args
        subject_id: str - the subject ID
        config_file: str - the config file path

    Returns
        List[Dict[str, Any]] - the blood metrics for each event
    """

    forms_df = data.get_all_subject_forms(
        subject_id=subject_id, config_file=config_file
    )

    blood_qa_forms = forms_df[
        forms_df["form_name"].str.contains("blood_sample_preanalytic_quality_assurance")
    ]

    required_variables: List[str] = [
        "chrblood_buffy_freeze",
        "chrblood_serum_freeze",
        "chrblood_plasma_freeze",
        "chrblood_wholeblood_freeze",
    ]

    results: List[Dict[str, Any]] = []

    for _, row in blood_qa_forms.iterrows():
        event_name = row["event_name"]
        form_data: Dict[str, Any] = row["form_data"]

        blood_draw_date = form_data.get("chrblood_drawdate", None)
        if blood_draw_date is not None:
            blood_draw_date = pd.to_datetime(blood_draw_date)

        result: Dict[str, Any] = {
            "subject_id": subject_id,
            "event_name": event_name,
            "blood_draw_date": blood_draw_date,
        }

        for variable in required_variables:
            variable_value = form_data.get(variable, None)
            result[variable] = variable_value

            binned_variable_name = f"{variable}_binned"
            # <30 mins, 30-60 mins, 60-90 mins, 90-120 mins, 120-150 mins, >150 mins
            if variable_value is None:
                binned_variable_value = None
            else:
                variable_value = int(variable_value)
                if variable_value < 30:
                    binned_variable_value = "lt30"
                elif variable_value < 60:
                    binned_variable_value = "30_60"
                elif variable_value < 90:
                    binned_variable_value = "60_90"
                elif variable_value < 120:
                    binned_variable_value = "90_120"
                elif variable_value < 150:
                    binned_variable_value = "120_150"
                else:
                    binned_variable_value = "gt150"

            result[binned_variable_name] = binned_variable_value

        # Skip appending if all required variables are None
        if all([result[variable] is None for variable in required_variables]):
            continue

        results.append(result)

    return results


def process_subject(params: Tuple[Path, str]) -> List[Dict[str, Any]]:
    """
    Wrapper function for the `get_subject_blood_status` function.

    Args:
        params (Tuple[Path, str]): A tuple containing the config file, subject ID

    Returns:
        List[Dict[str, Any]]: List of blood metrics for each event
    """
    config_file, subject_id = params
    subject_data = get_subject_blood_status(
        config_file=config_file, subject_id=subject_id
    )

    return subject_data


def consolidate_blood_metrics(config_file: Path) -> pd.DataFrame:
    """
    For each subject, get the blood metrics for each event.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        pd.DataFrame: DataFrame containing the blood metrics for each event for each subject.
    """
    logger.info("Consolidating blood metrics for each subject...")

    subject_ids = data.get_all_subjects(config_file=config_file)
    subjects_count = len(subject_ids)
    logger.info(f"Found {subjects_count} subjects.")

    num_processes = 8
    logger.info(f"Using {num_processes} processes.")

    params = [(config_file, subject_id) for subject_id in subject_ids]
    results = []

    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for result in pool.imap_unordered(process_subject, params):
                results.append(result)
                progress.update(task, advance=1)

    logger.info(f"Done consolidating blood metrics for {subjects_count} subjects.")

    # flatten the list of lists
    results = [item for sublist in results for item in sublist]

    blood_metrics_df = pd.DataFrame(results)

    return blood_metrics_df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    consolidated_blood_metrics_df = consolidate_blood_metrics(config_file=config_file)

    logger.info("Committing blood_metrics table to the database...")
    db.df_to_table(
        config_file=config_file,
        df=consolidated_blood_metrics_df,
        table_name="blood_metrics",
    )

    logger.info("Done!")
