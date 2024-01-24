from pathlib import Path
from datetime import datetime

import pandas as pd

from formsqc.helpers import db


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
