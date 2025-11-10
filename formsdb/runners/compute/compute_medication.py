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

import numpy as np
import pandas as pd
from psycopg2.extensions import AsIs, register_adapter
from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import db, utils, dpdash

# Addresses:
# sqlalchemy.exc.ProgrammingError: (psycopg2.ProgrammingError) can't adapt type 'numpy.int64'
register_adapter(np.int64, AsIs)

MODULE_NAME = "formsdb.runners.compute.medication"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


medication_forms: List[str] = [
    "past_pharmaceutical_treatment",
    "current_pharmaceutical_treatment_floating_med_125",
    "current_pharmaceutical_treatment_floating_med_2650",
]


# Check missing codes
def remove_missing_codes(value: Optional[str]) -> Optional[str]:
    """
    Remove missing codes from the value.

    Recognized missing codes: -9, -3
    Replaces missing codes with None.

    Args:
        value (Optional[Union[str, int]]): Value to check.

    Returns:
        Optional[Union[str, int]]: Value without missing codes.
    """
    if value is None:
        return None
    # Check if digit
    try:
        int(value)
    except ValueError:
        return value
    if int(value) in (-9, -3, 99999, 999999):
        return None
    return value


# Cast to datetime
# Reference:
# https://github.com/AMP-SCZ/utility/blob/15a5ef5b49d1e081ee0a549375f78bb26160d958/rpms_to_redcap.py#L55C1-L71C21
def handle_datetime(time_value: str) -> Optional[datetime]:
    """
    Handles different time formats from RPMS.

    This helps standardize the time formats to be used in the exported CSVs.

    Args:
        time_value (str): Time value to handle.

    Returns:
        datetime: Time value in datetime format.
    """
    # Try ISO format first
    try:
        datetime_val = datetime.fromisoformat(time_value)
    except ValueError as e:
        if len(time_value) == 10:
            try:
                # interview_date e.g. 11/30/2022
                datetime_val = datetime.strptime(time_value, "%m/%d/%Y")
            except ValueError:
                # psychs form e.g. 03/03/1903
                datetime_val = datetime.strptime(time_value, "%d/%m/%Y")
        elif len(time_value) > 10:
            # all other forms e.g. 1/05/2022 12:00:00 AM
            datetime_val = datetime.strptime(time_value, "%d/%m/%Y %I:%M:%S %p")
        else:
            raise ValueError(f"Unknown time format: {time_value}") from e

    # If date is 1909, return None
    # if datetime_val.year == 1903:
    #     return None
    if datetime_val.year == 1909:
        return None

    # Check if time is 09:09 (missing code)
    if datetime_val.hour == 9 and datetime_val.minute == 9:
        # Remove the time part
        datetime_val = datetime_val.replace(hour=0, minute=0, second=0, microsecond=0)

    # Similarly check for 03:03 and 03:33 (missing codes)
    if (datetime_val.hour == 3 and datetime_val.minute == 3) or (
        datetime_val.hour == 3 and datetime_val.minute == 33
    ):
        # Remove the time part
        datetime_val = datetime_val.replace(hour=0, minute=0, second=0, microsecond=0)

    return datetime_val


def get_subject_medication_info(
    config_file: Path, subject_id: str
) -> List[Dict[str, Any]]:
    """
    Compile medication data for a subject.

    Args:
        config_file (Path): Path to the configuration file.
        subject_id (str): Subject ID.

    Returns:
        List[Dict[str, Any]]: List of medication data for the subject.
            Contains the following keys:
                - subject_id
                - source_form
                - med_idx
                - med_name
                - med_class
                - med_indication
                - start_date
                - end_date
                - duration_days
                - med_use
                - med_frequency_per_month
                - med_compliance

    """
    subject_medication_data: List[Dict[str, Any]] = []

    try:
        subject_consent = data.get_subject_consent_dates(
            subject_id=subject_id, config_file=config_file
        )
    except data.NoSubjectConsentDateException:
        return subject_medication_data

    for medication_form in medication_forms:
        form_event_name = "floating_forms"
        if medication_form == "past_pharmaceutical_treatment":
            form_event_name = "screening"
        form_df = data.get_form_by_event(
            subject_id=subject_id,
            form_name=medication_form,
            event_name=form_event_name,
            config_file=config_file,
        )

        if form_df.empty:
            # print(f"No data for {medication_form}")
            continue

        form_df = utils.explode_col(
            df=form_df,
        )
        if "current_pharmaceutical_treatment" in medication_form:
            if medication_form == "current_pharmaceutical_treatment_floating_med_125":
                form_modified_date_var = "chrpharm_date_mod"
            elif (
                medication_form == "current_pharmaceutical_treatment_floating_med_2650"
            ):
                form_modified_date_var = "chrpharm_date_mod_2"
            else:
                raise ValueError(f"Unknown medication form: {medication_form}")
        else:
            form_modified_date_var = None

        if form_modified_date_var is not None:
            try:
                form_modified_date = form_df[form_modified_date_var].iloc[0]
            except KeyError:
                logger.warning(
                    f"Missing modified date variable "
                    f"{form_modified_date_var} for "
                    f"{medication_form} for subject "
                    f"{subject_id}"
                )
                form_modified_date = None
            form_modified_date = remove_missing_codes(form_modified_date)
            if form_modified_date is not None:
                form_modified_date = handle_datetime(form_modified_date)
            else:
                form_modified_date = None
                if medication_form != "past_pharmaceutical_treatment":
                    # Only warn for current forms, past forms do not have a modified date
                    logger.warning(
                        f"Missing modified date for {medication_form} for subject {subject_id}"
                    )
        else:
            form_modified_date = None

        med_idx = 0
        while med_idx < 25:
            stopped_medication = None
            med_idx += 1
            med_name_variable = f"chrpharm_med{med_idx}_name"
            if medication_form == "past_pharmaceutical_treatment":
                med_name_variable = f"{med_name_variable}_past"
            if med_name_variable not in form_df.columns:
                continue
            med_idx_str: str = str(form_df[med_name_variable].iloc[0])
            med_idx_str = med_idx_str.replace(".0", "")
            med_id = int(med_idx_str)
            # if med_name == 999:
            #     continue
            med_info = data.get_medication_info_by_id(
                config_file=config_file, med_id=med_id
            )
            if med_info is None:
                raise ValueError(f"Medication {med_id} not found in database")

            # priorotize first dose as start date over onset date for start date
            first_dose_date_variable = f"chrpharm_firstdose_med{med_idx}"
            onset_date_variable = f"chrpharm_med{med_idx}_onset"
            if medication_form == "past_pharmaceutical_treatment":
                first_dose_date_variable = f"{first_dose_date_variable}_past"
                onset_date_variable = f"{onset_date_variable}_past"

            start_date = None
            if first_dose_date_variable in form_df.columns:
                start_date = form_df[first_dose_date_variable].iloc[0]
                start_date = remove_missing_codes(start_date)
                if start_date is not None:
                    start_date = handle_datetime(start_date)
            if start_date is None and onset_date_variable in form_df.columns:
                start_date = form_df[onset_date_variable].iloc[0]
                start_date = remove_missing_codes(start_date)
                if start_date is not None:
                    start_date = handle_datetime(start_date)
            else:
                # print(f"Missing start date for med {med_idx}: {subject_id}@{medication_form}")
                pass

            # similarly, prioritize offset date over last use date for end date
            last_use_date_variable = f"chrpharm_lastuse_med{med_idx}"
            offset_date_variable = f"chrpharm_med{med_idx}_offset"
            # form_modified_date_variable = "chrpharm_date_mod"
            if medication_form == "past_pharmaceutical_treatment":
                last_use_date_variable = f"{last_use_date_variable}_past"
                offset_date_variable = f"{offset_date_variable}_past"
                # form_modified_date_variable = f"{form_modified_date_variable}_past"

            end_date = None
            if offset_date_variable in form_df.columns:
                end_date = form_df[offset_date_variable].iloc[0]
                end_date = remove_missing_codes(end_date)
                if end_date is not None:
                    end_date = handle_datetime(end_date)
            if end_date is None and last_use_date_variable in form_df.columns:
                end_date = form_df[last_use_date_variable].iloc[0]
                end_date = remove_missing_codes(end_date)
                if end_date is not None:
                    end_date = handle_datetime(end_date)
            # if end_date is None and form_modified_date_variable in form_df.columns:
            #     end_date = form_df[form_modified_date_variable].iloc[0]
            #     end_date = remove_missing_codes(end_date)
            #     if end_date is not None:
            #         end_date = handle_datetime(end_date)
            else:
                # print(f"Missing end date for med {med_idx}: {subject_id}@{medication_form}")
                pass

            if end_date is not None:
                stopped_medication = True

            med_compliance_variable = f"chrpharm_med{med_idx}_comp"
            med_final_compliance_variable = f"chrpharm_med{med_idx}_comp_2"
            if medication_form == "past_pharmaceutical_treatment":
                med_compliance_variable = f"{med_compliance_variable}_past"
                med_final_compliance_variable = f"{med_final_compliance_variable}_past"

            # Prioritize final compliance over initial compliance
            if med_final_compliance_variable in form_df.columns:
                med_compliance = form_df[med_final_compliance_variable].iloc[0]
            elif med_compliance_variable in form_df.columns:
                med_compliance = form_df[med_compliance_variable].iloc[0]
            else:
                med_compliance = None

            med_use_variable = f"chrpharm_med{med_idx}_use"
            if medication_form == "past_pharmaceutical_treatment":
                med_use_variable = f"{med_use_variable}_past"
            if med_use_variable in form_df.columns:
                med_use = form_df[med_use_variable].iloc[0]
                med_use = remove_missing_codes(med_use)
                if med_use is not None:
                    med_use = int(float(med_use))
            else:
                med_use = None

            dosage_variable = f"chrpharm_med{med_idx}_dosage"
            if medication_form == "past_pharmaceutical_treatment":
                dosage_variable = f"{dosage_variable}_past"
            if dosage_variable in form_df.columns:
                med_dosage = form_df[dosage_variable].iloc[0]
            else:
                med_dosage = None

            if med_dosage is None and med_use == 2:
                dosage_variable = f"chrpharm_med{med_idx}_dosage_2"
                if medication_form == "past_pharmaceutical_treatment":
                    dosage_variable = f"{dosage_variable}_past"
                if dosage_variable in form_df.columns:
                    med_dosage = form_df[dosage_variable].iloc[0]
                else:
                    med_dosage = None

            med_frequency_variable = f"chrpharm_med{med_idx}_frequency"
            if medication_form == "past_pharmaceutical_treatment":
                med_frequency_variable = f"{med_frequency_variable}_past"
            if med_frequency_variable in form_df.columns:
                med_frequency = form_df[med_frequency_variable].iloc[0]
                med_frequency = remove_missing_codes(med_frequency)
                if med_frequency is not None:
                    med_frequency = int(float(med_frequency))
            else:
                med_frequency = None

            med_indication_variable = f"chrpharm_med{med_idx}_indication"
            if medication_form == "past_pharmaceutical_treatment":
                med_indication_variable = f"{med_indication_variable}_past"

            med_indication_str = None
            med_indication = None
            if med_indication_variable in form_df.columns:
                med_indication = form_df[med_indication_variable].iloc[0]
                try:
                    med_indication = int(float(med_indication))
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid indication code for med {med_idx} "
                        f"({med_id}) in {medication_form} "
                        f"for subject {subject_id}: "
                        f"{med_indication}"
                    )
                    med_indication = None

                if med_indication is not None:
                    med_indication_str = data.get_dictionary_choice(
                        config_file=config_file,
                        variable_name=med_indication_variable,
                        choice=med_indication,
                    )

            # Free-text indication comment
            med_indication_comment_variable = f"chrpharm_med{med_idx}_other2"
            if medication_form == "past_pharmaceutical_treatment":
                med_indication_comment_variable = (
                    f"{med_indication_comment_variable}_past"
                )

            if med_indication_comment_variable in form_df.columns:
                med_indication_comment = form_df[med_indication_comment_variable].iloc[
                    0
                ]
                med_indication_comment = remove_missing_codes(med_indication_comment)
            else:
                med_indication_comment = None

            med_dosage = remove_missing_codes(med_dosage)
            med_compliance = remove_missing_codes(med_compliance)

            if med_dosage is None:
                med_dosage = pd.NA
            else:
                med_dosage = float(med_dosage)
                if med_dosage < 0:
                    logger.warning(
                        (
                            f"Negative dosage for med {med_idx} "
                            f"({med_id}: {med_dosage}) in "
                            f"{medication_form} for subject "
                            f"{subject_id}"
                        )
                    )
                    med_dosage = pd.NA

            # Check if medication is ongoing
            ongoing = None
            if end_date is not None and end_date.year == 1901:
                # Placeholder for ongoing medication
                ongoing = True
                end_date = None
                stopped_medication = False

            if "current_pharmaceutical_treatment" in medication_form:
                # timpoint_added_variable = f"chrpharm_med{med_idx}_tp"
                # timpoint_added = form_df[timpoint_added_variable].iloc[0]
                # timpoint_added = int(float(timpoint_added))

                months = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 18, 24]

                for month in months:
                    ongoing_variable = f"chrpharm_med{med_idx}_mo{month}"
                    if ongoing_variable in form_df.columns:
                        ongoing_variable_value = form_df[ongoing_variable].iloc[0]
                        ongoing_variable_value = int(float(ongoing_variable_value))
                        if ongoing_variable_value == 1:  # continuing
                            ongoing = True
                        elif ongoing_variable_value == 2:  # stopped
                            ongoing = False
                        elif ongoing_variable_value == 3:  # skipped
                            ongoing = None

                if ongoing is True:
                    stopped_medication = False
                    # logger.debug(
                    #     f"Medication {med_idx} "
                    #     f"({med_id}) in "
                    #     f"{medication_form} "
                    #     f"for subject {subject_id} "
                    #     f"is ongoing"
                    # )
                    # end_date = datetime(
                    #     1606, 6, 6
                    # )  # Placeholder for ongoing medication
                    end_date = form_modified_date

            if medication_form == "past_pharmaceutical_treatment":
                last_visit_interest = "screening"
            elif end_date is not None:
                last_visit_interest = data.get_closest_timepoint(
                    subject_id=subject_id,
                    date=end_date,
                    config_file=config_file,
                )
            elif start_date is not None:
                last_visit_interest = data.get_closest_timepoint(
                    subject_id=subject_id,
                    date=start_date,
                    config_file=config_file,
                )
            else:
                last_visit_interest = None

            start_days_from_consent = None
            end_days_from_consent = None
            if start_date is not None:
                start_days_from_consent = dpdash.get_days_between_dates(
                    consent_date=subject_consent,
                    event_date=start_date,
                )
                if start_date < subject_consent:
                    start_days_from_consent = -start_days_from_consent
            if end_date is not None:
                end_days_from_consent = dpdash.get_days_between_dates(
                    consent_date=subject_consent,
                    event_date=end_date,
                )
                if end_date < subject_consent:
                    end_days_from_consent = -end_days_from_consent

            if start_date is not None and end_date is not None:
                duration_days = (end_date - start_date).days
            else:
                duration_days = None

            raw_data = {
                "subject_id": subject_id,
                "source_form": medication_form,
                "med_idx": med_idx,
                "med_id": med_id,
                "med_name": med_info["med_name"],
                "med_class": med_info["med_class"],
                "med_indication": med_indication_str,
                "med_indication_comment": med_indication_comment,
                "med_dosage": med_dosage,
                "med_use": med_use,
                "med_frequency_per_month": med_frequency,
                "med_compliance": med_compliance,
                "start_date": start_date,
                "end_date": end_date,
                "stopped_medication": stopped_medication,
                "last_visit_interest": last_visit_interest,
                "start_days_from_consent": start_days_from_consent,
                "end_days_from_consent": end_days_from_consent,
                "duration_days": duration_days,
            }
            subject_medication_data.append(raw_data)

    # if len(subject_medication_data) == 0:
    #     raw_data = {
    #         "subject_id": subject_id,
    #         "med_idx": 999,
    #         "med_name": "NO MEDS",
    #         "med_class": "SPECIAL CODE"
    #     }
    #     subject_medication_data.append(raw_data)

    return subject_medication_data


def process_subject_wrapper(params: Tuple[Path, str]) -> List[Dict[str, Any]]:
    """
    A wrapper function to process a subject's medication data.
    """
    config_file, subject_id = params
    return get_subject_medication_info(config_file=config_file, subject_id=subject_id)


def compile_medication_data(config_file: Path) -> pd.DataFrame:
    """
    Retrieve all medication data for all subjects.

    Args:
        config_file (Path): Path to the configuration file.

    Returns:
        pd.DataFrame: DataFrame containing all medication data.
    """
    subject_ids = data.get_all_subjects(config_file=config_file)
    medication_raw_data: List[Dict[str, Any]] = []

    num_processes = 16
    params = [(config_file, subject_id) for subject_id in subject_ids]
    logger.info(f"Using {num_processes} processes")

    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for result in pool.imap_unordered(process_subject_wrapper, params):
                medication_raw_data.extend(result)
                progress.update(task, advance=1)

    medication_df = pd.DataFrame(medication_raw_data)

    # Cast duration_days, med_use and med_compliance to Optional[int]
    # ValueError: invalid literal for int() with base 10: '2.0'
    # TypeError: cannot safely cast non-equivalent float64 to int64
    medication_df["med_dosage"] = (
        pd.to_numeric(
            medication_df["med_dosage"], errors="coerce"
        )  # Convert valid strings to float
        # .round(0)  # Round to nearest integer (if needed)
        # .astype("Int64")  # Convert to nullable integer
    )
    medication_df["med_use"] = (
        pd.to_numeric(
            medication_df["med_use"], errors="coerce"
        )  # Convert valid strings to float
        .round(0)  # Round to nearest integer (if needed)
        .astype("Int64")  # Convert to nullable integer
    )
    medication_df["med_compliance"] = (
        pd.to_numeric(medication_df["med_compliance"], errors="coerce")
        .round(0)
        .astype("Int64")
    )
    medication_df["duration_days"] = (
        pd.to_numeric(medication_df["duration_days"], errors="coerce")
        .round(0)
        .astype("Int64")
    )

    # Sort by subject_id and start_date
    medication_df = medication_df.sort_values(by=["subject_id", "start_date"])

    return medication_df


def get_medication_output_dir(config_file: Path) -> Path:
    """
    Get the output directory for the medication data.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["medications_root"])
    return output_dir


def update_medication_data(
    config_file: Path,
) -> None:
    """
    Compile medication data for all subjects and update the database and CSV files.

    Args:
        config_file (Path): Path to the configuration file.
    """

    medication_df = compile_medication_data(config_file=config_file)

    output_dir = get_medication_output_dir(config_file=config_file)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "medication_summary.csv"

    medication_df.to_csv(summary_path, index=False)
    logger.info(f"Saved medication data to {summary_path}")

    logger.info("Updating medication data in the database...")
    medication_df = medication_df.convert_dtypes()

    db.df_to_table(
        df=medication_df,
        schema="forms_derived",
        table_name="medication_data",
        config_file=config_file,
        if_exists="replace",
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Compiling medication data...")
    update_medication_data(config_file=config_file)

    logger.info("Done!")
