"""
Module contain helper functions specific to this data pipeline
"""

from pathlib import Path
from datetime import datetime
from typing import List

import pandas as pd

from formsdb.helpers import db
from formsdb import constants


def get_network(config_file: Path, site: str) -> str:
    """
    Get the network of a site from the database.

    Args:
        config_file (Path): The path to the configuration file.
        site (str): The site ID.

    Returns:
        str: The network the site belongs to.
    """
    query = f"""
    SELECT network_id
    FROM site
    WHERE id = '{site}'
    """

    network = db.fetch_record(config_file=config_file, query=query)

    if network is None:
        raise ValueError(f"Site {site} not found in database.")

    return network


def check_if_subject_exists(config_file: Path, subject_id: str) -> bool:
    """
    Check if a subject exists in the database.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        bool: True if the subject exists, False otherwise.
    """
    query = f"""
    SELECT id FROM subjects WHERE id = '{subject_id}';
    """

    subject = db.fetch_record(config_file=config_file, query=query)

    if subject is None:
        return False
    else:
        return True


def subject_uses_rpms(config_file: Path, subject_id: str) -> bool:
    """
    Check if a subject uses RPMS (as opposed to REDCap).

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        bool: True if the subject uses RPMS, False if the subject uses REDCap.

    Raises:
        ValueError: If the network is not recognized.
        ValueError: If the site is not found in the database.
    """
    query = f"""
    SELECT network_id FROM site WHERE id = '{subject_id[0:2]}';
    """

    network_id = db.fetch_record(config_file=config_file, query=query)

    if network_id is None:
        raise ValueError(f"Site {subject_id[0:2]} not found in database.")

    if network_id == "PRESCIENT":
        return True
    elif network_id == "ProNET":
        return False
    else:
        raise ValueError(f"Network {network_id} not recognized.")


class NoSubjectConsentDateException(Exception):
    """
    Custom exception for when no consent date is found in the database.
    """

    pass


def get_subject_consent_dates(config_file: Path, subject_id: str) -> datetime:
    """
    Get the consent date of a subject from the database.

    Uses the `informed_consent_run_sheet` form to get the consent date (chric_consent_date).
    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        datetime: The consent date.
    """
    query = f"""
    SELECT form_data ->> 'chric_consent_date' as consent_date
    FROM forms
    WHERE subject_id = '{subject_id}' AND
        form_name = 'informed_consent_run_sheet' AND
        form_data ? 'chric_consent_date';
    """

    date = db.fetch_record(config_file=config_file, query=query)

    if date is None:
        raise NoSubjectConsentDateException("No consent date found in the database.")
    date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")

    return date


def get_all_subject_forms(config_file: Path, subject_id: str) -> pd.DataFrame:
    """
    Get all forms for a subject from the database.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        pd.DataFrame: A DataFrame containing all forms for the subject.
    """
    query = f"""
    SELECT * FROM forms WHERE subject_id = '{subject_id}';
    """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def get_days_since_consent(
    config_file: Path, subject_id: str, event_date: datetime
) -> int:
    """
    Get the number of days since the subject's consent date.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        event_date (datetime): The event date.

    Returns:
        int: The number of days since the subject's consent date.
    """
    consent_date = get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )

    return (event_date - consent_date).days + 1


def get_upenn_event_date(
    config_file: Path, subject_id: str, event_name: str
) -> datetime:
    """
    Get the date of an event for a subject from the database, based on
    UPEENN's form_data.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        event_name (str): The name of the event.

    Returns:
        datetime: The event date.

    Raises:
        ValueError: If the event is not found in the database.
    """
    query = f"""
    SELECT form_data ->> 'session_date' as session_date
    FROM upenn_forms
    WHERE subject_id = '{subject_id}' AND
        event_name LIKE '%%{event_name}%%' AND
        form_data ? 'session_date';
    """

    date = db.fetch_record(config_file=config_file, query=query)

    if date is None:
        raise ValueError(
            f"No event {event_name} found in the database for subject {subject_id}."
        )
    date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")

    return date


def get_upenn_days_since_consent(
    config_file: Path, subject_id: str, event_name: str
) -> int:
    """
    Get the number of days since the subject's consent date for an event.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        event_name (str): The name of the event.

    Returns:
        int: The number of days since the subject's consent date for the event.
    """
    consent_date = get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )
    event_date = get_upenn_event_date(
        config_file=config_file, subject_id=subject_id, event_name=event_name
    )

    return (event_date - consent_date).days + 1


def make_df_dpdash_ready(df: pd.DataFrame, subject_id: str) -> pd.DataFrame:
    """
    Make a DataFrame DPDash ready, by adding DPDash required columns and the subject_id column.

    Args:
        df (pd.DataFrame): Input DataFrame.
        subject_id (str): The subject ID.

    Returns:
        pd.DataFrame: Output DataFrame.
    """
    dp_dash_required_cols = constants.dp_dash_required_cols + ["subject_id"]

    for col in dp_dash_required_cols:
        if col not in df.columns:
            df[col] = ""

    cols: List[str] = df.columns.tolist()
    cols = [col for col in cols if col not in dp_dash_required_cols]

    df = df[dp_dash_required_cols + cols]
    df["subject_id"] = subject_id

    # Check if [day] is filled in, else fill it in with 1 to n
    # replace empty strings with NA
    df.replace("", pd.NA, inplace=True)
    if df["day"].isnull().values.any():  # type: ignore
        df["day"] = df["day"].ffill()
        df["day"] = df["day"].bfill()
        df["day"] = df["day"].fillna(1)

    return df
