#!/usr/bin/env python
"""
Compile all medication data from different sources into a single table.
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
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.compute.compute_nda_upload_eligibility"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

required_variables: Dict[str, Dict] = {
    "chrcrit_part": {
        "form_name": "inclusionexclusion_criteria_review",
        "event_name": "screening",
        "value": {
            "type": "only",  # Has only 1 potential field
            "datatype": int,
            "field_name": ["chrcrit_part"],
        },
    },
    "guid": {
        "form_name": "guid_form",
        "event_name": "screening",
        "value": {
            "type": "any",  # Can have multiple potential fields
            "datatype": str,
            "field_name": ["chrguid_guid", "chrguid_pseudoguid"],
        },
    },
    "chrdemo_age_mos": {
        "form_name": "sociodemographics",
        "event_name": "baseline",
        "value": {
            "type": "any",
            "datatype": int,
            "field_name": [
                "chrdemo_age_mos_chr",
                "chrdemo_age_mos_hc",
                "chrdemo_age_mos2",
            ],
        },
    },
    "chrdemo_sexassigned": {
        "form_name": "sociodemographics",
        "event_name": "baseline",
        "value": {
            "type": "only",
            "datatype": int,
            "field_name": ["chrdemo_sexassigned"],
        },
    },
    "chric_consent_date": {
        "form_name": "informed_consent_run_sheet",
        "event_name": "screening",
        "value": {
            "type": "only",
            "datatype": datetime,
            "field_name": ["chric_consent_date"],
        },
    },
    "chrdemo_interview_date": {
        "form_name": "sociodemographics",
        "event_name": "baseline",
        "value": {
            "type": "only",
            "datatype": datetime,
            "field_name": ["chrdemo_interview_date"],
        },
    },
}

required_conditions: Dict[str, Dict] = {
    "no cohort": {
        "form_name": "inclusionexclusion_criteria_review",
        "event_name": "screening",
        "condition_type": "assert",  # assert if value in accepted_values
        "value": {
            "field_name": "chrcrit_part",
            "accepted_values": [1, 2],
            "datatype": int,
        },
    },
    "not chrcrit_included": {
        "form_name": "inclusionexclusion_criteria_review",
        "event_name": "screening",
        "condition_type": "assert",
        "value": {
            "field_name": "chrcrit_included",
            "accepted_values": [1],
            "datatype": int,
        },
    },
    "chrcrit QC issue": {
        "form_name": "inclusionexclusion_criteria_review",
        "event_name": "screening",
        "condition_type": "owen_qc",  # assert if value in accepted_values
    },
    "psychs_p1p8 QC issue": {
        "form_name": "psychs_p1p8",
        "event_name": "screening",
        "condition_type": "owen_qc",  # assert if value in accepted_values
    },
    "psychs_p9ac32 QC issue": {
        "form_name": "psychs_p9ac32",
        "event_name": "screening",
        "condition_type": "owen_qc",  # assert if value in accepted_values
    },
}


def cast(value: str, to_type: type) -> Optional[Any]:
    """
    Cast a value to a specific type, return None if it fails

    Also ignores known missing value codes.

    Args:
        value (str): The value to cast.
        to_type (type): The type to cast to.

    Returns:
        Optional[Any]: The casted value or None if casting fails.
    """
    missing_values = ["-3", "1903-03-03", "-9", "1909-09-09"]
    if value in missing_values:
        return None
    try:
        if to_type == datetime:
            if isinstance(value, (int, float)):
                # Assume it's a timestamp
                return datetime.fromtimestamp(value)
            elif isinstance(value, str):
                # Try to parse string date
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                # If no format matched, raise error
                raise ValueError(f"Unknown date format: {value}")
        return to_type(value)
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to cast value {value} to {to_type}: {e}")
        return None


def get_missing_fields(
    subject_id: str, required_variables: Dict[str, Dict], config_file: Path
) -> Tuple[str, List[str]]:
    """
    Check for missing required fields for a subject.

    Args:
        subject_id (str): Subject ID.
        required_variables (Dict[str, Dict]): Dictionary of required variables.
        config_file (Path): Path to the config file.

    Returns:
        Tuple[str, List[str]]: Subject ID and list of missing variable names.
    """
    missing_variables = []

    for required_variable, variable_info in required_variables.items():
        form_name: str = variable_info["form_name"]
        event_name: str = variable_info["event_name"]
        expected_datatype = variable_info["value"]["datatype"]

        variable_type: str = variable_info["value"]["type"]
        if variable_type == "only":
            field_name: str = variable_info["value"]["field_name"][0]
            field_value = data.get_variable(
                config_file=config_file,
                subject_id=subject_id,
                form_name=form_name,
                event_name=event_name,
                variable_name=field_name,
            )
            # Cast to expected datatype
            if field_value is not None:
                # logger.info(f"{required_variable}: {field_value}")
                field_value = cast(field_value, expected_datatype)
            else:
                # logger.warning(f"{required_variable}: {field_value}")
                missing_variables.append(required_variable)

        elif variable_type == "any":
            field_names: List[str] = variable_info["value"]["field_name"]
            variable_found = False
            for field_name in field_names:
                field_value = data.get_variable(
                    config_file=config_file,
                    subject_id=subject_id,
                    form_name=form_name,
                    event_name=event_name,
                    variable_name=field_name,
                )
                if field_value is not None:
                    # logger.info(f"{required_variable}: {field_value}")
                    field_value = cast(field_value, expected_datatype)
                    variable_found = True
                    break
            if not variable_found:
                # logger.warning(f"{required_variable}: None")
                missing_variables.append(required_variable)

    return (subject_id, missing_variables)


def get_failed_conditions(
    subject_id: str, required_conditions: Dict[str, Dict], config_file: Path
) -> Tuple[str, List[str]]:
    """
    Check for failed conditions for a subject.

    Args:
        subject_id (str): Subject ID.
        required_conditions (Dict[str, Dict]): Dictionary of required conditions.
        config_file (Path): Path to the config file.

    Returns:
        Tuple[str, List[str]]: Subject ID and list of failed condition names.
    """
    failed_conditions = []

    for condition_name, condition_info in required_conditions.items():
        form_name: str = condition_info["form_name"]
        event_name: str = condition_info["event_name"]
        condition_type: str = condition_info["condition_type"]

        if condition_type == "assert":
            field_name: str = condition_info["value"]["field_name"]
            accepted_values: List[Any] = condition_info["value"]["accepted_values"]
            expected_datatype = condition_info["value"]["datatype"]

            field_value = data.get_variable(
                config_file=config_file,
                subject_id=subject_id,
                form_name=form_name,
                event_name=event_name,
                variable_name=field_name,
            )
            if field_value is not None:
                field_value = cast(field_value, expected_datatype)

            if field_value not in accepted_values:
                failed_conditions.append(condition_name)

        elif condition_type == "owen_qc":
            # Owen's QC logic
            qc_info = data.get_form_qc(
                subject_id=subject_id,
                form_name=form_name,
                event_name=event_name,
                config_file=config_file,
            )
            qc_issue: bool = qc_info.get("qc_issue", True)
            # qc_comment = qc_info.get("comment", None)

            if qc_issue:
                # failed_conditions.append(f"{condition_name} ({qc_comment})")
                failed_conditions.append(condition_name)

    return (subject_id, failed_conditions)


def process_subject_wrapper(
    args: Tuple[str, Dict[str, Dict], Dict[str, Dict], Path],
) -> Tuple[str, List[str]]:
    """
    Wrapper for multiprocessing to unpack arguments.
    """
    subject_id, required_variables, required_conditions, config_file = args
    _, missing_fields = get_missing_fields(
        subject_id=subject_id,
        required_variables=required_variables,
        config_file=config_file,
    )
    # Append 'missing ' to each missing field for clarity
    missing_fields = [f"missing {field}" for field in missing_fields]

    _, failed_conditions = get_failed_conditions(
        subject_id=subject_id,
        required_conditions=required_conditions,
        config_file=config_file,
    )
    return (subject_id, missing_fields + failed_conditions)


def compute_nda_upload_eligibility(
    config_file: Path,
) -> pd.DataFrame:
    """
    Compute the NDA upload eligibility for a list of subjects.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        pd.DataFrame: DataFrame with subject IDs and their missing fields/failed conditions.
    """
    subjects = data.get_all_subjects(config_file=config_file)
    valid_subjects: List[str] = []
    invalid_subjects: List[Tuple[str, List[str]]] = []

    num_processes = 4
    params = [
        (subject_id, required_variables, required_conditions, config_file)
        for subject_id in subjects
    ]

    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for result in pool.imap_unordered(process_subject_wrapper, params):
                subject_id, failed = result
                if failed:
                    invalid_subjects.append((subject_id, failed))
                else:
                    valid_subjects.append(subject_id)
                progress.update(task, advance=1)

    invalid_subjects_df = pd.DataFrame(
        invalid_subjects, columns=["subject_id", "issues"]
    )

    exploded = invalid_subjects_df.explode("issues")
    bool_table = pd.crosstab(
        exploded["subject_id"], exploded["issues"]
    ).astype(bool)
    result_df = invalid_subjects_df.join(bool_table, on="subject_id")

    # Add column 'nda_upload_eligible' with False for invalid subjects
    result_df["nda_upload_eligible"] = False

    # Add valid_subjects to result_df with 'nda_upload_eligible' as True
    valid_df = pd.DataFrame(valid_subjects, columns=["subject_id"])
    valid_df["nda_upload_eligible"] = True
    result_df = pd.concat([result_df, valid_df], ignore_index=True, sort=False)

    # Fill NaN with False for boolean columns
    bool_columns = result_df.columns.difference(["subject_id", "issues"])
    result_df[bool_columns] = result_df[bool_columns].fillna(False)

    return result_df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Computing NDA upload eligibility...")
    df = compute_nda_upload_eligibility(config_file=config_file)

    logger.info("Uploading to database...")
    db.df_to_table(
        df=df,
        table_name="nda_upload_eligibility",
        schema="forms_derived",
        config_file=config_file,
        if_exists="replace",
    )

    logger.info("Done!")
