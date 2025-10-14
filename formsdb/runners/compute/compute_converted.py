#!/usr/bin/env python
"""
Updates the subject_converted table in the database with the converted status of subjects.
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
from datetime import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.compute.compute_converted"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def count_converted(config_file: Path) -> int:
    """
    Count the number of subjects that have been converted.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        int: Number of subjects that have been converted.
    """
    query = (
        "SELECT COUNT(*) FROM forms_derived.conversion_status WHERE converted = 'True';"
    )
    converted_count_r = db.fetch_record(config_file=config_file, query=query)
    if converted_count_r is None:
        raise ValueError("No converted subjects found in the database.")
    converted_count = int(converted_count_r)

    return converted_count


def check_if_converted_conversion_form(subject_id: str, config_file: Path) -> bool:
    """
    Checks subject's conversion forms for conversion status.

    Args:
        subject_id (str): The subject ID.
        config_file (Path): Path to the config file.

    Returns:
        bool: Whether the subject has been converted.
    """
    all_forms_df = data.get_all_subject_forms(
        config_file=config_file, subject_id=subject_id
    )

    conversion_df = all_forms_df[
        all_forms_df["form_name"].str.contains("conversion_form")
    ]

    if conversion_df.empty:
        return False

    conversion_df.reset_index(drop=True, inplace=True)

    for idx, _ in conversion_df.iterrows():
        form_r = conversion_df.iloc[idx, :]  # type: ignore
        form_data: Dict[str, Any] = form_r["form_data"]

        variable = "chrconv_conv"

        if variable in form_data:
            if form_data[variable] == 1:
                return True

    return False


def get_conversion_date(subject_id: str, config_file: Path) -> Optional[datetime]:
    """
    Checks subject's conversion forms for conversion date.
    Args:
        subject_id (str): The subject ID.
        config_file (Path): Path to the config file.

    Returns:
        Optional[datetime]: The date of conversion.
    """
    all_forms_df = data.get_all_subject_forms(
        config_file=config_file, subject_id=subject_id
    )

    conversion_df = all_forms_df[
        all_forms_df["form_name"].str.contains("conversion_form")
    ]

    if conversion_df.empty:
        return None

    conversion_df.reset_index(drop=True, inplace=True)

    for idx, _ in conversion_df.iterrows():
        form_r = conversion_df.iloc[idx, :]  # type: ignore
        form_data: Dict[str, Any] = form_r["form_data"]

        variable = "chrconv_interview_date"

        if variable in form_data:
            conversion_date = form_data[variable]
            conversion_date_d = datetime.strptime(conversion_date, "%Y-%m-%dT%H:%M:%S")

            # Ignore if converted date is earlier than 1950
            if conversion_date_d.year < 1950:
                return None

            return conversion_date_d

    return None


def compute_converted(config_file: Path) -> pd.DataFrame:
    """
    Check if subjects have been converted.

    Args:
        config_file (Path): Path to the config file.
        visit_order (List[str]): List of visit names in the order they were collected.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame containing the converted status of subjects.
    """
    visit_status_df = pd.DataFrame(
        columns=["subject_id", "converted", "converted_visit"]
    )
    logger.info("Computing if subjects got converted...")

    subject_ids = data.get_all_subjects(config_file=config_file)
    overidden_subjects = data.get_overrides(
        config_file=config_file, measure="conversion"
    )

    logger.info(f"Overridden subjects: {overidden_subjects}")

    with utils.get_progress_bar() as progress:
        task = progress.add_task("[red]Processing...", total=len(subject_ids))

        for subject_id in subject_ids:
            progress.update(task, advance=1, description=f"Processing {subject_id}...")
            converted = False
            converted_visit = None
            conversion_info_source: Optional[str] = None

            if subject_id in overidden_subjects:
                converted = True
                converted_visit = "overidden"

                logger.info(f"Subject {subject_id} is overidden to be converted.")

            converted = check_if_converted_conversion_form(
                subject_id=subject_id, config_file=config_file
            )
            if converted:
                conversion_info_source = "conversion_form"

            conversion_date = get_conversion_date(
                subject_id=subject_id, config_file=config_file
            )
            if conversion_date is not None and (
                converted_visit is np.nan or converted_visit is None
            ):
                converted_visit = data.get_closest_timepoint(
                    subject_id=subject_id,
                    date=conversion_date,
                    config_file=config_file,
                )
            if conversion_date is not None and "conversion_form" not in str(
                conversion_info_source
            ):
                conversion_info_source = (
                    str(conversion_info_source) + ", conversion_form"
                )
                converted = True

            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                visit_status_df = pd.concat(
                    [
                        visit_status_df,
                        pd.DataFrame(
                            {
                                "subject_id": [subject_id],
                                "converted": [converted],
                                "converted_visit": [converted_visit],
                                "conversion_date": [conversion_date],
                                "conversion_info_source": [conversion_info_source],
                            }
                        ),
                    ]
                )

    logger.info(f"Done computing converted status for {len(subject_ids)} subjects.")

    return visit_status_df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    CONVERTED_COUNT = count_converted(config_file=config_file)
    logger.info(f"Found {CONVERTED_COUNT} converted subjects.")

    converted_status_df = compute_converted(config_file)

    db.df_to_table(
        config_file=config_file,
        df=converted_status_df,
        schema="forms_derived",
        table_name="conversion_status",
    )

    UPDATED_CONVERTED_COUNT = count_converted(config_file=config_file)
    logger.info(f"Found {UPDATED_CONVERTED_COUNT} converted subjects after update.")
    logger.info(
        f"Added {UPDATED_CONVERTED_COUNT - CONVERTED_COUNT} converted subjects."
    )

    logger.info("Done!")
