from pathlib import Path
from datetime import datetime
from typing import List

import pandas as pd

from formsqc.helpers import db
from formsqc import constants


def get_network(config_file: Path, site: str) -> str:
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
    query = f"""
    SELECT id FROM subjects WHERE id = '{subject_id}';
    """

    subject = db.fetch_record(config_file=config_file, query=query)

    if subject is None:
        return False
    else:
        return True


def subject_uses_rpms(config_file: Path, subject_id: str) -> bool:
    query = f"""
    SELECT network_id FROM site WHERE id = '{subject_id[0:2]}';
    """

    network_id = db.fetch_record(config_file=config_file, query=query)

    if network_id is None:
        raise Exception("No network found in the database.")

    if network_id == "PRESCIENT":
        return True
    elif network_id == "ProNET":
        return False
    else:
        raise Exception("Invalid network.")


class NoSubjectConsentDateException(Exception):
    pass


def get_subject_consent_dates(config_file: Path, subject_id: str) -> datetime:
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
    query = f"""
    SELECT * FROM forms WHERE subject_id = '{subject_id}';
    """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def get_days_since_consent(
    config_file: Path, subject_id: str, event_date: datetime
) -> int:
    consent_date = get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )

    return (event_date - consent_date).days + 1


def get_upenn_event_date(
    config_file: Path, subject_id: str, event_name: str
) -> datetime:
    query = f"""
    SELECT form_data ->> 'session_date' as session_date
    FROM upenn_forms
    WHERE subject_id = '{subject_id}' AND
        event_name LIKE '%%{event_name}%%' AND
        form_data ? 'session_date';
    """

    date = db.fetch_record(config_file=config_file, query=query)

    if date is None:
        raise Exception("No event date found in the database.")
    date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")

    return date


def get_upenn_days_since_consent(
    config_file: Path, subject_id: str, event_name: str
) -> int:
    consent_date = get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )
    event_date = get_upenn_event_date(
        config_file=config_file, subject_id=subject_id, event_name=event_name
    )

    return (event_date - consent_date).days + 1


def make_df_dpdash_ready(df: pd.DataFrame, subject_id: str) -> pd.DataFrame:
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
