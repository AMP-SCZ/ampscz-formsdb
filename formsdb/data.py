"""
Module contain helper functions specific to this data pipeline
"""

import json
import logging
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from formsdb import constants
from formsdb.helpers import db, utils

logger = logging.getLogger(__name__)


def get_overrides(config_file: Path, measure: str) -> List[str]:
    """
    Get the subject IDs that have been overridden for a measure.

    Args:
        config_file (Path): The path to the configuration file.
        measure (str): The measure.

    Returns:
        List[str]: A list of subject IDs that have been overridden for the measure.
    """
    config_params = utils.config(path=config_file, section="overrides")
    overrides = config_params.get(measure, "")

    subject_ids = overrides.split(",")
    subject_ids = [subject_id.strip() for subject_id in subject_ids]
    # Remove ' if present
    subject_ids = [subject_id.strip().strip("'") for subject_id in subject_ids]

    return subject_ids


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


def get_all_subjects(
    config_file: Path,
    network: Optional[str] = None,
) -> List[str]:
    """
    Get all subjects from the database.

    Args:
        config_file (Path): The path to the configuration file.
        network (Optional[str], optional): The network to filter by.
            Defaults to None.

    Returns:
        List[str]: A list of all subjects.
    """

    if network:
        query = f"""
        SELECT subjects.id FROM subjects
        INNER JOIN site ON subjects.site_id = site.id
        WHERE site.network_id = '{network}'
        ORDER BY id;
        """
    else:
        query = """
        SELECT id FROM subjects
        ORDER BY id;
        """

    subjects = db.execute_sql(config_file=config_file, query=query)

    return subjects["id"].tolist()


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


def get_subject_network(config_file: Path, subject_id: str) -> str:
    """
    Get the network of a subject from the database.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        str: The network the subject belongs to.
    """
    query = f"""
    SELECT network_id FROM site WHERE id = '{subject_id[0:2]}';
    """

    network = db.fetch_record(config_file=config_file, query=query)

    if network is None:
        raise ValueError(f"Site {subject_id[0:2]} not found in database.")

    return network


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

    network_id = get_subject_network(config_file=config_file, subject_id=subject_id)

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


def get_subject_age(config_file: Path, subject_id: str) -> Optional[int]:
    """
    Get the age of the subject.

    Uses the `chrdemo_age_mos_chr` and 'chrdemo_age_mos_hc` fields
    to calculate the age of the subject.
    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        Optional[int]: The age of the subject in yrs.
    """

    variables: List[str] = ["chrdemo_age_mos_chr", "chrdemo_age_mos_hc"]

    for variable in variables:
        query = f"""
        SELECT form_data ->> '{variable}' as age
        FROM forms
        WHERE subject_id = '{subject_id}' AND
            form_name = 'sociodemographics' AND
            form_data ? '{variable}';
        """

        age = db.fetch_record(config_file=config_file, query=query)

        if age is not None:
            break

    if age is None:
        return None

    age = int(int(age) / 12)

    return age


def subject_has_consent_date(subject_id: str, config_file: Path) -> bool:
    """
    Check if a subject has a consent date

    Args:
        subject_id: The subject ID
        config_file: The path to the config file

    Returns:
        True if the subject has a consent date, otherwise False
    """

    try:
        get_subject_consent_dates(config_file=config_file, subject_id=subject_id)
        return True
    except NoSubjectConsentDateException:
        return False


def subject_is_included(subject_id: str, config_file: Path) -> bool:
    """
    Check if a subject is included in the study.

    Based on 'chrcrit_included' variable from 'inclusionexclusion_criteria_review'
    form.

    Args:
        subject_id: The subject ID
        config_file: The path to the config file

    Returns:
        True if the subject is included, False is excluded
    """

    form_df = get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    # Get 'inclusionexclusion_criteria_review' form
    form_id = "inclusionexclusion_criteria_review"

    icr_df = form_df[form_df["form_name"] == form_id]
    icr_df.reset_index(drop=True, inplace=True)
    icr_df = utils.explode_col(df=icr_df, col="form_data")

    # Check if 'chrcrit_included' is 1
    try:
        included = icr_df["chrcrit_included"].iloc[0]
    except KeyError:
        included = False

    if included == 1:
        return True
    else:
        return False


def subject_is_excluded(subject_id: str, config_file: Path) -> bool:
    """
    Check if a subject is excluded from the study.

    Based on 'chrcrit_included' variable from 'inclusionexclusion_criteria_review'
    form.

    Args:
        subject_id: The subject ID
        config_file: The path to the config file

    Returns:
        True if the subject is excluded, False is included
    """

    form_df = get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    # Get 'inclusionexclusion_criteria_review' form
    form_id = "inclusionexclusion_criteria_review"

    icr_df = form_df[form_df["form_name"] == form_id]
    icr_df.reset_index(drop=True, inplace=True)
    icr_df = utils.explode_col(df=icr_df, col="form_data")

    # Check if 'chrcrit_included' is 1
    try:
        included = icr_df["chrcrit_included"].iloc[0]
    except KeyError:
        return False

    if included == 0:
        return True
    else:
        return False


@lru_cache(maxsize=128)
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
    SELECT COALESCE(form_data ->> 'interview_date', form_data ->> 'session_date') AS session_date
    FROM upenn_forms
    WHERE subject_id = '{subject_id}' AND
        event_name LIKE '%%{event_name}%%' AND
        (form_data ? 'session_date' OR form_data ? 'interview_date');

    """

    date = db.fetch_record(config_file=config_file, query=query)

    if date is None:
        raise ValueError(
            f"No event {event_name} found in the database for subject {subject_id}."
        )
    try:
        date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
    except ValueError:  # Try for 6/19/23 format
        date = datetime.strptime(date, "%m/%d/%y")

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
    dp_dash_required_cols = constants.dp_dash_required_cols
    df = df.copy()

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

        # Fill na with 1 to n
        if df["day"].isnull().values.any():  # type: ignore
            for i, row in df.iterrows():
                if pd.isna(row["day"]):
                    df.at[i, "day"] = i + 1  # type: ignore

    return df


def get_subject_guid(config_file: Path, subject_id: str) -> Optional[str]:
    """
    Get the GUID for a subject.

    Args:
        config_file: Path to the config file.
        subject_id: Subject ID.

    Returns:
        GUID for the subject.
    """

    forms_df = get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    guid_form_df = forms_df[forms_df["form_name"] == "guid_form"]

    if guid_form_df.empty:
        return None

    guid_form_df = utils.explode_col(df=guid_form_df, col="form_data")

    try:
        # guid variable: chrguid_guid
        guid = guid_form_df["chrguid_guid"].iloc[0]
    except KeyError:
        return None

    return guid


@lru_cache(maxsize=128)
def get_all_rpms_entry_status(
    config_file: Path,
    subject_id: str,
) -> pd.DataFrame:
    """
    Get all the entry status for a given form for a subject.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID
        form_name: The form name
        event_name: The event name

    Returns:
        Dict[str, Any]: The entry status
    """

    query = f"""
    SELECT * FROM rpms_entry_status
    WHERE subject_id = '{subject_id}'
    """

    entry_status_df = db.execute_sql(config_file=config_file, query=query)

    return entry_status_df


def rpms_form_has_missing_data(
    config_file: Path,
    subject_id: str,
    form_name: str,
    event_name: Optional[str] = None,
) -> Optional[bool]:
    """
    Check if a subject has missing data for a given form.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID
        form_name: The form name

    Returns:
        Optional[bool]: True if the subject has missing data, otherwise False.
            None if form does not exist for the subject.
    """

    status_form_df = get_all_rpms_entry_status(
        config_file=config_file, subject_id=subject_id
    )

    status_form_df = status_form_df[status_form_df["redcap_form_name"] == form_name]

    if event_name:
        status_form_df = status_form_df[
            status_form_df["redcap_event_name"].str.contains(event_name)
        ]

    if status_form_df.empty:
        return None

    status = status_form_df["CompletionStatus"].iloc[0]

    # RMPS completion status
    # 0: Incomplete
    # 1: Partial
    # 2: Complete
    # 3: N/A
    # 4: Missing
    if status == 4 or status == 3:
        return True

    return False


def form_has_missing_data(
    config_file: Path,
    subject_id: str,
    form_name: str,
    event_name: Optional[str] = None,
) -> Optional[bool]:
    """
    Check if a subject has missing data for a given form.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID
        form_name: The form name

    Returns:
        Optional[bool]: True if the subject has missing data, otherwise False.
            None if form does not exist for the subject.
    """
    if subject_uses_rpms(config_file=config_file, subject_id=subject_id):
        return rpms_form_has_missing_data(
            config_file=config_file,
            subject_id=subject_id,
            form_name=form_name,
            event_name=event_name,
        )

    form_df = get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    form_df = form_df[form_df["form_name"] == form_name]
    if event_name:
        form_df = form_df[form_df["event_name"].str.contains(f"{event_name}_")]
    form_df.reset_index(drop=True, inplace=True)

    if form_df.empty:
        return None

    form_df = utils.explode_col(df=form_df, col="form_data")

    form_abbrv = constants.form_name_to_abbrv[form_name]
    missing_variable = f"{form_abbrv}_missing"

    try:
        missing = form_df[missing_variable].iloc[0]
    except KeyError:
        return False

    if missing == 1:
        return True
    else:
        return False


def get_all_rpms_form_status_for_event(
    config_file: Path,
    subject_id: str,
    event_name: str,
) -> Dict[str, int]:
    """
    Get the form status for a subject's REDCap event.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID
        event_name: The REDCap event name

    Returns:
        Dict[str, int]: A dictionary containing the form status for the event.
            Legend:
            - 0: Not started -> RPMS 0
            - 1: In progress  -> RPMS 1
            - 2: Completed  -> RPMS 2, 3, 4
    """

    status_form_df = get_all_rpms_entry_status(
        config_file=config_file, subject_id=subject_id
    )

    status_form_df = status_form_df[
        status_form_df["redcap_event_name"] == event_name
    ]

    if status_form_df.empty:
        return {}

    # substitute RPMS completion status with REDCap completion status
    status_form_df.loc[:, "CompletionStatus"] = status_form_df[
        "CompletionStatus"
    ].replace({2: 2, 3: 2, 4: 2})

    # mimic redcap form name
    # append '_complete' to the form name
    status_form_df.loc[:, "redcap_form_name"] = (
        status_form_df["redcap_form_name"] + "_complete"
    )

    form_data: Dict[str, int] = (
        status_form_df[["redcap_form_name", "CompletionStatus"]]
        .set_index("redcap_form_name")
        .to_dict()["CompletionStatus"]
    )

    return form_data


def get_all_form_status_for_event(
    config_file: Path,
    subject_id: str,
    event_name: str,
) -> Dict[str, int]:
    """
    Get the form status for a subject's REDCap event.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID
        event_name: The REDCap event name

    Returns:
        Dict[str, int]: A dictionary containing the form status for the event.
            Legend:
            - 0: Not started
            - 1: In progress
            - 2: Completed
    """

    if subject_uses_rpms(config_file=config_file, subject_id=subject_id):
        return get_all_rpms_form_status_for_event(
            config_file=config_file, subject_id=subject_id, event_name=event_name
        )

    form_df = get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    # Filter by event name and form_name = 'uncategorized'
    form_df = form_df[form_df["event_name"].str.contains(f"{event_name}_")]
    form_df = form_df[form_df["form_name"] == "uncategorized"]

    # reset index
    form_df.reset_index(drop=True, inplace=True)

    if form_df.empty:
        raise ValueError(
            f"No 'uncategorized' form for subject {subject_id} and event {event_name}"
        )

    # Read JSON from form_data column as Dict
    form_data: Dict[str, int] = form_df["form_data"].iloc[0]

    return form_data


def form_is_complete(
    config_file: Path,
    subject_id: str,
    form_name: str,
    event_name: str,
) -> Optional[bool]:
    """
    Check if a form is complete for a subject.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID
        form_name: The form name
        event_name: The event name

    Returns:
        Optional[bool]: True if the form is complete, otherwise False.
            None if form_complete is not found for the subject.

    Note:
        If the form is complete, it could still have missing data.
    """

    form_status = get_all_form_status_for_event(
        config_file=config_file, subject_id=subject_id, event_name=event_name
    )

    form_abbrv = constants.form_name_to_abbrv[form_name]
    complete_variable = f"{form_abbrv}_complete"

    if complete_variable not in form_status:
        complete_variable = f"{form_name}_complete"
        if complete_variable not in form_status:
            return None

    completion_status = form_status[complete_variable]
    if completion_status == 2:
        # Note form could still have missing data
        return True
    else:
        # Form not marked complete
        return False


def get_subject_gender(config_file: Path, subject_id: str) -> Optional[str]:
    """
    Get the sex assigned at birth for the subject.

    Parameters
        - config_file: Path to the configuration file.
        - subject_id: The subject ID to

    Returns
        - The subject's sex
    """

    query = f"""
SELECT
    CASE
        WHEN form_data ->> 'chrdemo_sexassigned' = '1' THEN 'Male'
        WHEN form_data ->> 'chrdemo_sexassigned' = '2' THEN 'Female'
        ELSE 'Other'
    END AS sex
FROM
    forms
WHERE
    form_name = 'sociodemographics'
    AND form_data ? 'chrdemo_sexassigned'
    AND form_data ->> 'chrdemo_sexassigned' IS NOT NULL
    AND subject_id = '{subject_id}'
"""

    subject_gender = db.fetch_record(config_file=config_file, query=query)

    return subject_gender


# Cohort specific functions

@lru_cache(maxsize=None)
def get_subject_cohort(
    config_file: Path,
    subject_id: str,
) -> Optional[str]:
    """
    Get the cohort for a subject.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID

    Returns:
        Optional[str]: The cohort for the subject.
    """

    form_df = get_all_subject_forms(config_file=config_file, subject_id=subject_id)

    # Get 'inclusionexclusion_criteria_review' form
    form_id = "inclusionexclusion_criteria_review"

    icr_df = form_df[form_df["form_name"] == form_id]
    icr_df.reset_index(drop=True, inplace=True)
    icr_df = utils.explode_col(df=icr_df, col="form_data")

    # Check if 'chrcrit_part' is present
    cohort = None
    try:
        cohort = icr_df["chrcrit_part"].iloc[0]
    except KeyError:
        # Try looking for arm information in event_name column
        event_names = form_df["event_name"].unique()
        for event_name in event_names:
            if "_arm_1" in event_name:
                cohort = 1
                break
            elif "_arm_2" in event_name:
                cohort = 2
                break

    if cohort == 1:
        return "CHR"
    elif cohort == 2:
        return "HC"
    else:
        raise ValueError(f"Unknown cohort {cohort} for subject {subject_id}")


def is_subject_chr(
    config_file: Path,
    subject_id: str,
) -> bool:
    """
    Check if a subject is in the CHR cohort.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID

    Returns:
        bool: True if the subject is in the CHR cohort, otherwise False.
    """

    cohort = get_subject_cohort(config_file=config_file, subject_id=subject_id)

    if cohort == "CHR":
        return True
    else:
        return False


def is_subject_hc(
    config_file: Path,
    subject_id: str,
) -> bool:
    """
    Check if a subject is in the HC cohort.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID

    Returns:
        bool: True if the subject is in the HC cohort, otherwise False.
    """

    cohort = get_subject_cohort(config_file=config_file, subject_id=subject_id)

    if cohort == "HC":
        return True
    else:
        return False


# Recruitment Status


def get_subject_recruitment_status(config_file: Path, subject_id: str) -> Optional[str]:
    """
    Get the recruitment status for a subject.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID

    Returns:
        Optional[str]: The recruitment status for the subject.
    """

    query = f"""
    SELECT recruitment_status
    FROM recruitment_status
    WHERE subject_id = '{subject_id}'
    """

    recruitment_status = db.fetch_record(config_file=config_file, query=query)

    return recruitment_status


def get_subject_withdrawal_status(
    config_file: Path,
    subject_id: str,
) -> Optional[str]:
    """
    Get the withdrawal status for a subject.

    Args:
        config_file: The path to the config file
        subject_id: The subject ID

    Returns:
        Optional[str]: The withdrawal status for the subject.
    """

    query = f"""
    SELECT withdrawal_status
    FROM subject_removed
    WHERE subject_id = '{subject_id}'
    """

    withdrawal_status = db.fetch_record(config_file=config_file, query=query)

    return withdrawal_status


# Timepoint / Visit / Event Dates


def estimate_event_date(
    subject_id: str, event: str, config_file: Path
) -> Optional[datetime]:
    """
    Infers the events date.

    Args:
        subject_id (str): The subject ID.
        event (str): The event name.
        config_file (Path): The path to the config file.

    Returns:
        Optional[datetime]: The date of the event.
    """

    forms_df = get_all_subject_forms(subject_id=subject_id, config_file=config_file)

    forms_df = get_all_subject_forms(subject_id=subject_id, config_file=config_file)
    visit_df = forms_df[forms_df["event_name"].str.contains(f"{event}_")]

    if visit_df.empty:
        return None

    for _, row in visit_df.iterrows():
        form_name = row["form_name"]

        if form_name == "sociodemographics":
            continue

        if "digital_biomarkers" in form_name:
            continue

        form_data_r: Dict[str, Any] = row["form_data"]
        form_variables = form_data_r.keys()

        date_variables = [v for v in form_variables if "date" in v]

        for date_variable in date_variables:
            date = form_data_r[date_variable]

            # Validate date
            if not utils.validate_date(date):
                continue
            else:
                date_ts: pd.Timestamp = pd.to_datetime(date)
                if date_ts < datetime(2019, 1, 1):
                    continue
                date_dt = date_ts.to_pydatetime()
                return date_dt

    return None


def get_forms_cohort_timepoint_map(
    config_file: Path,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Returns a dictionary containing the forms for each cohort and timepoint.

    Args:
        config_file: The path to the config file

    Returns:
        Dict[str, Dict[str, List[str]]]: A dictionary containing the
            forms for each cohort and timepoint.
    """

    config_params = utils.config(path=config_file, section="data")
    forms_cohort_timepoint_map_path = config_params["forms_cohort_timepoint_map"]

    with open(forms_cohort_timepoint_map_path, "r", encoding="utf-8") as f:
        forms_cohort_timepoint_map = json.load(f)

    return forms_cohort_timepoint_map


def get_subject_timepoints(subject_id: str, config_file: Path) -> List[str]:
    """
    Get all timepoints for a subject.

    Args:
        subject_id: Subject ID.
        config_file: Path to the config file.

    Returns:
        List of timepoints / events / visits.
    """
    forms_cohort_timepoint_map = get_forms_cohort_timepoint_map(config_file=config_file)
    try:
        cohort = get_subject_cohort(config_file=config_file, subject_id=subject_id)
    except ValueError:
        cohort = None
    if cohort is None:
        logger.warning(f"No cohort found for subject: {subject_id}")
        cohort = "CHR"  # Assume CHR if cohort is not found.
    timepoints = forms_cohort_timepoint_map[cohort.lower()].keys()
    return list(timepoints)


def get_subject_visit_date_map(
    config_file: Path, subject_id: str
) -> Dict[str, datetime]:
    """
    Get a dictionary mapping visit names to visit dates for a subject.

    Args:
        config_file: Path to the config file.
        subject_id: The subject ID.

    Returns:
        A dictionary mapping visit names to visit dates.
    """

    visit_date_map = {}

    subject_timepoints = get_subject_timepoints(
        config_file=config_file, subject_id=subject_id
    )

    for timepoint in subject_timepoints:
        date_estimate = estimate_event_date(
            config_file=config_file, subject_id=subject_id, event=timepoint
        )
        visit_date_map[timepoint] = date_estimate

    return visit_date_map


def get_closest_timepoint(
    subject_id: str, date: datetime, config_file: Path
) -> Optional[str]:
    """
    Get the closest timepoint to a given date for a subject.

    Args:
        subject_id: Subject ID.
        date: The date.
        config_file: Path to the config file.

    Returns:
        The closest timepoint.
    """
    visit_date_map = get_subject_visit_date_map(
        config_file=config_file, subject_id=subject_id
    )

    # Drop keys with None values.
    visit_date_map = {k: v for k, v in visit_date_map.items() if v is not None}

    if not visit_date_map:
        return None

    closest_timepoint = min(
        visit_date_map.keys(), key=lambda x: abs(visit_date_map[x] - date)
    )

    return closest_timepoint


def get_subject_latest_visit_started(
    config_file: Path,
    subject_id: str,
) -> Optional[str]:
    """
    Returns the timepoint of the most recent form completed by the subject.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): The subject ID.

    Returns:
        Optional[str]: The timepoint of the most recent form completed by the subject.
    """

    qurery = f"""
        SELECT timepoint FROM subject_visit_status
        WHERE subject_id = '{subject_id}';
    """

    timepoint = db.fetch_record(config_file=config_file, query=qurery)

    return timepoint
