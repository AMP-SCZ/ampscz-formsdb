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
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
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
    query = "SELECT COUNT(*) FROM conversion_status WHERE converted = 'True';"
    try:
        converted_count_r = db.fetch_record(config_file=config_file, query=query)
        if converted_count_r is None:
            raise ValueError("No converted subjects found in the database.")
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"Error: {e}")
        converted_count_r = 0
    converted_count = int(converted_count_r)

    return converted_count


def check_if_converted(subject_id: str, config_file: Path) -> bool:
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
            return conversion_date_d

    return None


def get_converted_date_rpms(subject_id: str, config_file: Path) -> Optional[datetime]:
    """
    Checks subject's conversion forms for conversion status.

    Specific to subjects that use RPMS

    Args:
        subject_id (str): The subject ID.
        config_file (Path): Path to the config file.

    Returns:
        bool: Whether the subject has been converted.
    """
    query = f"""
        SELECT * FROM client_status WHERE subject_id = '{subject_id}'
    """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return None

    if df["Transition / Conversion"][0] is not None:
        conversion_date = df["Transition / Conversion"][0]
        conversion_date_dt = datetime.strptime(
            conversion_date, "%d/%m/%Y"
        )  # Updated format string
        return conversion_date_dt

    return None


def get_converted_visit(
    df: pd.DataFrame, visit_order: List[str], debug: bool = False
) -> Optional[str]:
    """
    Check if a subject has been converted.

    Uses the psychs_p9ac32_fu form to determine if a subject has been converted.

    Args:
        df (pd.DataFrame): DataFrame containing the forms data.
        visit_order (List[str]): List of visit names in the order they were collected.
        debug (bool, optional): Whether to print debug messages. Defaults to False.

    Returns:
        Optional[str]: The visit at which the subject converted, if any.
    """
    psychs_df = df[df["form_name"].str.contains("psychs_p9ac32_fu")]

    if psychs_df.shape[0] == 0:
        return None

    for visit in visit_order:
        visit_psychs_df = psychs_df[psychs_df["event_name"].str.contains(f"{visit}_")]
        if visit_psychs_df.shape[0] == 0:
            if debug:
                print(f"No psychs_p9ac32_fu form found for {visit}.")
            continue

        form_r = visit_psychs_df.iloc[0, :]
        form_data: Dict[str, Any] = form_r["form_data"]

        form_variables = list(form_data.keys())

        variable = "chrpsychs_fu_ac1_conv"
        if variable in form_variables:
            if form_data[variable] == 1:
                if debug:
                    print(f"Subject converted at {visit}.")
                return visit
        else:
            if debug:
                print(f"{variable} not found in {visit}.")

    return None


def get_converted_visit_rpms(subject_id: str, config_file: Path) -> Optional[str]:
    """
    Check's ClientStatus table for the converted visit for RPMS subjects.

    Args:
        subject_id (str): The subject ID.
        config_file (Path): Path to the config file.

    Returns:
        str: The converted visit.
    """

    converted_date = get_converted_date_rpms(
        subject_id=subject_id, config_file=config_file
    )
    if converted_date is None:
        return None
    closest_timepoint = data.get_closest_timepoint(
        subject_id=subject_id, date=converted_date, config_file=config_file
    )

    return closest_timepoint


def compute_converted(
    config_file: Path, visit_order: List[str], debug: bool = False
) -> pd.DataFrame:
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
            conversion_info_source = None

            if subject_id in overidden_subjects:
                converted = True
                converted_visit = "overidden"

                logger.info(f"Subject {subject_id} is overidden to be converted.")

            df = data.get_all_subject_forms(
                config_file=config_file, subject_id=subject_id
            )
            converted_visit = get_converted_visit(
                df=df, visit_order=visit_order, debug=debug
            )
            if converted_visit is None:
                converted_visit = np.nan
                converted = check_if_converted(
                    subject_id=subject_id, config_file=config_file
                )
                if converted:
                    conversion_info_source = "conversion_form"
            else:
                converted = True
                conversion_info_source = "psychs_p9ac32_fu_form"

            if data.subject_uses_rpms(subject_id=subject_id, config_file=config_file):
                converted_visit_rpms = get_converted_visit_rpms(
                    subject_id=subject_id, config_file=config_file
                )
                if converted_visit_rpms is not None:
                    converted_visit = converted_visit_rpms
                    converted = True
                    conversion_info_source = "client_status"
                conversion_date = get_converted_date_rpms(
                    subject_id=subject_id, config_file=config_file
                )
            else:
                conversion_date = get_conversion_date(
                    subject_id=subject_id, config_file=config_file
                )
                if conversion_date is not None and converted_visit is np.nan:
                    converted_visit = data.get_closest_timepoint(
                        subject_id=subject_id,
                        date=conversion_date,
                        config_file=config_file,
                    )

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

    converted_status_df = compute_converted(
        config_file, visit_order=constants.visit_order, debug=False
    )

    db.df_to_table(
        config_file=config_file,
        df=converted_status_df,
        table_name="conversion_status",
    )

    UPDATED_CONVERTED_COUNT = count_converted(config_file=config_file)
    logger.info(f"Found {UPDATED_CONVERTED_COUNT} converted subjects after update.")
    logger.info(
        f"Added {UPDATED_CONVERTED_COUNT - CONVERTED_COUNT} converted subjects."
    )

    logger.info("Done!")
