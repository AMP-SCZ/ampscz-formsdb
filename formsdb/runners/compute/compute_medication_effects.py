#!/usr/bin/env python
"""
Compile medication effects - active medications - during various modality timepoints.

Uses medication data compiled earlier in the pipeline, to infer medication effects.
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

# import random
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union, Set

import numpy as np
import pandas as pd
from psycopg2.extensions import AsIs, register_adapter
from rich.logging import RichHandler

from formsdb import data, constants
from formsdb.helpers import db, utils

# Addresses:
# sqlalchemy.exc.ProgrammingError: (psycopg2.ProgrammingError) can't adapt type 'numpy.int64'
register_adapter(np.int64, AsIs)

MODULE_NAME = "formsdb.runners.compute.medication_effects"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

modalities_info: Dict[str, Dict[str, Any]] = {
    "mri": {
        "timepoints": ["baseline", "month_2"],
        "form_name": "mri_run_sheet",
        "date_variable": "chrmri_entry_date",
        "time_variable": "chrmri_starttime",
    },
    "eeg": {
        "timepoints": ["baseline", "month_2"],
        "form_name": "eeg_run_sheet",
        "date_variable": "chreeg_interview_date",
        "time_variable": "chreeg_start",
    },
    "cognition": {
        "timepoints": ["baseline", "month_2", "month_6", "month_12", "month_24"],
        "form_name": "penncnb",
        "date_variable": "chrpenn_interview_date",
    },
    "psychs_screening": {
        "timepoints": ["screening"],
        "form_name": "psychs_p1p8",
        "date_variable": [
            "chrpsychs_scr_interview_date",
            "hcpsychs_scr_interview_date",
        ],
    },
    "psychs": {
        "timepoints": [
            "baseline",
            "month_1",
            "month_2",
            "month_3",
            "month_6",
            "month_12",
            "month_18",
            "month_24",
        ],
        "form_name": "psychs_p1p8_fu",
        "date_variable": ["chrpsychs_fu_interview_date", "hcpsychs_fu_interview_date"],
    },
    "bprs": {
        "timepoints": [
            "baseline",
            "month_1",
            "month_2",
            "month_3",
            "month_4",
            "month_5",
            "month_6",
            "month_7",
            "month_8",
            "month_9",
            "month_10",
            "month_11",
            "month_12",
            "month_18",
            "month_24",
        ],
        "form_name": "bprs",
        "date_variable": ["chrbprs_interview_date"],
    },
}


def get_medication_status(
    medication_df: pd.DataFrame,
    target_date: Union[str, datetime],
) -> Tuple[List[str], List[str], List[str]]:
    """
    Returns the medications currently being taken and those taken before the target date.

    Args:
        medication_df (pd.DataFrame): DataFrame containing medication information.
        target_date (datetime): Target date to check medication status.

    Returns:
        Tuple[List[str], List[str], List[str]]: Tuple containing the:
            list of medications currently being taken,
            list of potential medications currently being taken,
            list of those taken before the target date
    """
    # return_variable = 'med_class'
    # return_variable = 'med_name'
    return_variable = "med_id"
    current_date = pd.to_datetime(target_date)
    medication_df["start_date"] = pd.to_datetime(medication_df["start_date"])
    medication_df["end_date"] = pd.to_datetime(
        medication_df["end_date"], errors="coerce"
    )

    # Medications currently being taken
    currently_taking = medication_df[
        (medication_df["start_date"] <= current_date)
        & (medication_df["end_date"] >= current_date)
    ][return_variable].tolist()

    # Potential medications currently being taken
    potential_currently_taking = medication_df[
        (medication_df["start_date"] <= current_date)
        & (medication_df["end_date"].isna())
    ][return_variable].tolist()

    # Medications taken at least once before the date
    taken_before = medication_df[
        (medication_df["start_date"] <= current_date)
        | (medication_df["end_date"] <= current_date)
    ][return_variable].tolist()

    # remove duplicates
    currently_taking = list(set(currently_taking))
    taken_before = list(set(taken_before))

    return currently_taking, potential_currently_taking, taken_before


def get_medication_duaraion_i(
    medication_df: pd.DataFrame,
    target_medication_idx: str,
    target_date: Union[str, datetime],
) -> Optional[Tuple[float, float, int]]:
    """
    Returns the duration of medication intake.

    Args:
        medication_df (pd.DataFrame): DataFrame containing medication information.
        target_medication_name (str): Target medication name.
        target_date (datetime): Target date to check medication status.

    Returns:
        Optional[Tuple[float, float, int]]: Tuple containing:
            - Average complied dosage per day
            - Average dosage per day
            - Duration in days
    """
    target_date = pd.to_datetime(target_date)
    medication_df = medication_df[medication_df["med_id"] == target_medication_idx]

    if medication_df.empty:
        return None

    dates: Set[str] = set()
    dosage_by_date: Dict[str, List[float]] = {}
    complied_dosage_by_date: Dict[str, List[float]] = {}
    for _, row in medication_df.iterrows():
        start_date = row["start_date"]
        end_date = row["end_date"]
        dosage = row["med_dosage"]
        compliance = row["med_compliance"]

        if pd.isna(end_date) or pd.isna(start_date):
            continue

        start_date_dt = pd.to_datetime(start_date)

        if start_date_dt > target_date:
            continue

        end_date = min(end_date, target_date)
        date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d")
        dates.update(date_range)

        for date in date_range:
            if date not in dosage_by_date:
                dosage_by_date[date] = []
                complied_dosage_by_date[date] = []
            dosage_by_date[date].append(dosage)
            try:
                _complied_dosage_by_date: float = (compliance / 100) * dosage
            except TypeError:
                _complied_dosage_by_date = pd.NA  # type: ignore[assignment]
            complied_dosage_by_date[date].append(_complied_dosage_by_date)

    total_dosage_by_date: Dict[str, float] = {}
    complied_total_dosage_by_date: Dict[str, float] = {}

    for date, dosages in dosage_by_date.items():
        try:
            total_dosage_by_date[date] = sum(dosages)
        except TypeError:
            total_dosage_by_date[date] = pd.NA  # type: ignore[assignment]
    for date, dosages in complied_dosage_by_date.items():
        try:
            complied_total_dosage_by_date[date] = sum(dosages)
        except TypeError:
            complied_total_dosage_by_date[date] = pd.NA  # type: ignore[assignment]

    duration = len(dates)
    if duration == 0:
        return pd.NA, pd.NA, pd.NA  # type: ignore[return]

    avg_dosage = sum(total_dosage_by_date.values()) / duration
    complied_avg_dosage = sum(complied_total_dosage_by_date.values()) / duration

    return complied_avg_dosage, avg_dosage, duration


# Check missing codes
def remove_missing_codes(value):
    """
    Remove missing codes from the value.
    """
    if value is None:
        return None
    # Check if digit
    try:
        int(value)
    except ValueError:
        return value
    if int(value) in (-9, -3):
        return None
    return value


def get_subject_medication_effect_info(
    config_file: Path, subject_id: str
) -> List[Dict[str, Any]]:
    """
    Get medication effect information for a specific subject.

    Args:
        config_file (Path): Path to the configuration file.
        subject_id (str): Subject ID to retrieve medication effect information for.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing medication effect
            information for the subject.
    """
    subject_results: List[Dict[str, Any]] = []
    subject_medication_data = data.get_subject_medication_data(
        subject_id=subject_id,
        config_file=config_file,
    )

    no_meds_id = 999
    # If subject_medication_data has 999, check if no other medications are present
    # else, remove 999
    if no_meds_id in subject_medication_data["med_id"].values:
        temp_medication_data = subject_medication_data[
            subject_medication_data["med_id"] != no_meds_id
        ]
        if temp_medication_data.empty:
            # Subject has no medications except for 999
            pass
            # # Subject has no medications
            # logger.warning(
            #     f"Subject {subject_id} has no medications. Skipping."
            # )
            # return subject_results
        else:
            logger.warning(
                f"Subject {subject_id} has other medications despite med_id=999 presence. \
Removing 999."
            )
            subject_medication_data = subject_medication_data[
                subject_medication_data["med_id"] != no_meds_id
            ]

    med_info = data.get_all_medication_info(
        config_file=config_file,
    )

    # Check if subject has taken meds that affect lifetime use
    inconclusive_medications = [777, 888]
    subject_inconclusive_medication_data = subject_medication_data[
        subject_medication_data["med_id"].isin(inconclusive_medications)
    ]
    if not subject_inconclusive_medication_data.empty:
        # Cast start_date to datetime
        subject_inconclusive_medication_data = (
            subject_inconclusive_medication_data.copy()
        )
        subject_inconclusive_medication_data["start_date"] = pd.to_datetime(
            subject_inconclusive_medication_data["start_date"]
        )
        inconclusive_meds_start_date = subject_inconclusive_medication_data[
            "start_date"
        ].min()
        lifetime_use_inconclusive = True
        # logger.warning(
        #     f"Subject {subject_id} has taken medication with med_id 777 or 888. \
    # Lifetime use inconclusive since {inconclusive_meds_start_date}."
    # )
    else:
        lifetime_use_inconclusive = False
        inconclusive_meds_start_date = None

    for modality_info in modalities_info:
        modality = modality_info
        timepoints = modalities_info[modality]["timepoints"]
        form_name = modalities_info[modality]["form_name"]
        date_variables = modalities_info[modality]["date_variable"]
        # Check if time_variable is provided
        time_variable = modalities_info[modality].get("time_variable", None)

        for timepoint in timepoints:
            # print(f"Processing modality: {modality} - Timepoint: {timepoint}")
            subject_date = None
            if isinstance(date_variables, list):
                for date_variable in date_variables:
                    subject_date = data.get_variable(
                        config_file=config_file,
                        subject_id=subject_id,
                        form_name=form_name,
                        event_name=timepoint,
                        variable_name=date_variable,
                    )
                    subject_date = remove_missing_codes(subject_date)
                    if subject_date is not None:
                        break
            else:
                date_variable = date_variables
                subject_date = data.get_variable(
                    config_file=config_file,
                    subject_id=subject_id,
                    form_name=form_name,
                    event_name=timepoint,
                    variable_name=date_variable,
                )
                subject_date = remove_missing_codes(subject_date)

            if time_variable is not None:
                subject_time = data.get_variable(
                    config_file=config_file,
                    subject_id=subject_id,
                    form_name=form_name,
                    event_name=timepoint,
                    variable_name=time_variable,
                )
                subject_time = remove_missing_codes(subject_time)  # 1900-01-01T17:06:00
                # Only use time information
                if subject_time is not None and subject_date is not None:
                    # Convert to datetime object
                    if isinstance(subject_time, str):
                        subject_time = subject_time.rsplit("T", maxsplit=1)[-1]
                    subject_date_dt = pd.to_datetime(f"{subject_date} {subject_time}")
                elif subject_date is not None:
                    subject_date_dt = pd.to_datetime(subject_date)
                else:
                    subject_date_dt = None
            else:
                if subject_date is not None:
                    subject_date_dt = pd.to_datetime(subject_date)
                else:
                    subject_date_dt = None

            subject_consent_date = data.get_subject_consent_dates(
                config_file=config_file, subject_id=subject_id
            )

            if subject_date_dt is None:
                days_from_consent = None
            else:
                days_from_consent = (subject_date_dt - subject_consent_date).days + 1

            if subject_date_dt is None:
                # print(f"Skipping {modality} - {timepoint} for {subject_id}")
                continue
            # print(f"Processing {modality} - {timepoint} for {subject_id} on {subject_date_dt}")
            currently_taking, potential_currently_taking, taken_before = (
                get_medication_status(
                    medication_df=subject_medication_data, target_date=subject_date_dt
                )
            )

            all_involed_med_idxs = list(
                set(currently_taking + potential_currently_taking + taken_before)
            )

            for med_idx in all_involed_med_idxs:
                try:
                    med_class = med_info[int(med_idx)]["med_class"]
                except KeyError:
                    # If the medication index is not found in the med_info, skip it
                    logger.warning(
                        f"{subject_id}: Medication index {med_idx} not found in med_info."
                    )
                    continue
                under_influence = med_idx in currently_taking
                could_be_under_influence = med_idx in potential_currently_taking
                lifetime_use = med_idx in taken_before

                result = get_medication_duaraion_i(
                    medication_df=subject_medication_data,
                    target_medication_idx=med_idx,
                    target_date=subject_date_dt,
                )

                if result is None:
                    complied_dosage, dosage_prescribed, duration_prescribed = (
                        None,
                        None,
                        None,
                    )
                else:
                    complied_dosage, dosage_prescribed, duration_prescribed = result

                prescribed_eq_dosage_for_day = pd.NA
                complied_equivalent_drug_dose_for_day = pd.NA

                if pd.isna(dosage_prescribed) or pd.isna(duration_prescribed):
                    ap_equivalent_drug_dose_prescribed = pd.NA
                    ap_equivalent_drug_dose_taken = pd.NA
                    ad_equivalent_drug_dose_prescribed = pd.NA
                    ad_equivalent_drug_dose_taken = pd.NA
                    bd_equivalent_drug_dose_prescribed = pd.NA
                    bd_equivalent_drug_dose_taken = pd.NA
                else:
                    if med_class == "ANTIPSYCHOTIC":
                        ap_standard_equivalent_drug_dose = (
                            constants.med_idx_drug_equivalent_dose.get(
                                int(med_idx), None
                            )
                        )
                        if ap_standard_equivalent_drug_dose is not None:
                            prescribed_eq_dosage_for_day = dosage_prescribed * (
                                50 / ap_standard_equivalent_drug_dose
                            )
                            ap_equivalent_drug_dose_prescribed = (
                                prescribed_eq_dosage_for_day * duration_prescribed
                            )

                            if complied_dosage is not None:
                                complied_equivalent_drug_dose_for_day = (
                                    complied_dosage
                                    * (50 / ap_standard_equivalent_drug_dose)
                                )
                            else:
                                complied_equivalent_drug_dose_for_day = pd.NA
                            ap_equivalent_drug_dose_taken = (
                                complied_equivalent_drug_dose_for_day
                                * duration_prescribed
                            )
                        else:
                            # print(f"Missing AP Standard Equivalent Drug Dose for {med_idx}")
                            ap_equivalent_drug_dose_prescribed = pd.NA
                            ap_equivalent_drug_dose_taken = pd.NA
                    else:
                        ap_equivalent_drug_dose_prescribed = pd.NA
                        ap_equivalent_drug_dose_taken = pd.NA

                    if med_class == "ANTIDEPRESSANT":
                        ad_med_name: str = med_info[int(med_idx)]["med_name"]
                        # Get first word
                        ad_med_name = ad_med_name.split("/")[0].lower()
                        ad_med_name = ad_med_name.split(" ")[0]
                        ad_standard_equivalent_drug_dose = (
                            constants
                            .antidepressant_fluoxetine_40mg_drug_equivalent_dose
                            .get(
                                ad_med_name,
                                None,
                            )
                        )
                        if ad_standard_equivalent_drug_dose is not None:
                            prescribed_eq_dosage_for_day = dosage_prescribed * (
                                50 / ad_standard_equivalent_drug_dose
                            )
                            ad_equivalent_drug_dose_prescribed = (
                                prescribed_eq_dosage_for_day * duration_prescribed
                            )
                            if complied_dosage is not None:
                                complied_equivalent_drug_dose_for_day = (
                                    complied_dosage
                                    * (50 / ad_standard_equivalent_drug_dose)
                                )
                            else:
                                complied_equivalent_drug_dose_for_day = pd.NA
                            ad_equivalent_drug_dose_taken = (
                                complied_equivalent_drug_dose_for_day
                                * duration_prescribed
                            )
                        else:
                            # print(f"Missing AD Standard Equivalent Drug Dose for {ad_med_name}")
                            ad_equivalent_drug_dose_prescribed = pd.NA
                            ad_equivalent_drug_dose_taken = pd.NA
                    else:
                        ad_equivalent_drug_dose_prescribed = pd.NA
                        ad_equivalent_drug_dose_taken = pd.NA

                    if med_class == "BENZODIAZEPINE":
                        bd_med_name: str = med_info[int(med_idx)]["med_name"]
                        # Get first word
                        bd_med_name = bd_med_name.split("/")[0].lower()
                        bd_med_name = bd_med_name.split(" ")[0]
                        bd_standard_equivalent_drug_dose = (
                            constants.benzodiazepine_diazepam_5mg_drug_equivalent_dose.get(
                                bd_med_name, None
                            )
                        )
                        if bd_standard_equivalent_drug_dose is not None:
                            prescribed_eq_dosage_for_day = dosage_prescribed * (
                                5 / bd_standard_equivalent_drug_dose
                            )
                            bd_equivalent_drug_dose_prescribed = (
                                prescribed_eq_dosage_for_day * duration_prescribed
                            )
                            if complied_dosage is not None:
                                complied_equivalent_drug_dose_for_day = (
                                    complied_dosage
                                    * (5 / bd_standard_equivalent_drug_dose)
                                )
                            else:
                                complied_equivalent_drug_dose_for_day = pd.NA
                            bd_equivalent_drug_dose_taken = (
                                complied_equivalent_drug_dose_for_day
                                * duration_prescribed
                            )
                        else:
                            print(
                                f"Missing BD Standard Equivalent Drug Dose for {bd_med_name}"
                            )
                            bd_equivalent_drug_dose_prescribed = pd.NA
                            bd_equivalent_drug_dose_taken = pd.NA
                    else:
                        bd_equivalent_drug_dose_prescribed = pd.NA
                        bd_equivalent_drug_dose_taken = pd.NA

                if under_influence:
                    under_influence_v = 1
                elif could_be_under_influence:
                    under_influence_v = pd.NA
                    prescribed_eq_dosage_for_day = pd.NA
                    complied_equivalent_drug_dose_for_day = pd.NA
                else:
                    under_influence_v = 0
                    prescribed_eq_dosage_for_day = 0
                    complied_equivalent_drug_dose_for_day = 0

                if lifetime_use:
                    lifetime_use_v = 1
                else:
                    lifetime_use_v = 0

                if lifetime_use_inconclusive:
                    if inconclusive_meds_start_date is None:
                        lifetime_use_v = pd.NA
                    elif subject_date_dt >= inconclusive_meds_start_date:
                        if lifetime_use_v == 0:
                            lifetime_use_v = pd.NA

                try:
                    result = {
                        "subject_id": subject_id,
                        "modality": modality,
                        "timepoint": timepoint,
                        "date": subject_date_dt,
                        "days_from_consent": days_from_consent,
                        "med_id": med_idx,
                        "med_name": med_info[int(med_idx)]["med_name"],
                        "med_class": med_class,
                        "ap_equivalent_drug_dose_prescribed": ap_equivalent_drug_dose_prescribed,
                        "ap_equivalent_drug_dose_taken": ap_equivalent_drug_dose_taken,
                        "ad_equivalent_drug_dose_prescribed": ad_equivalent_drug_dose_prescribed,
                        "ad_equivalent_drug_dose_taken": ad_equivalent_drug_dose_taken,
                        "bd_equivalent_drug_dose_prescribed": bd_equivalent_drug_dose_prescribed,
                        "bd_equivalent_drug_dose_taken": bd_equivalent_drug_dose_taken,
                        "duration_prescribed_days": duration_prescribed,
                        "prescribed_equivalent_drug_dose_for_day": prescribed_eq_dosage_for_day,
                        "complied_equivalent_drug_dose_for_day": (
                            complied_equivalent_drug_dose_for_day
                        ),
                        "current_use": under_influence_v,
                        "lifetime_use": lifetime_use_v,
                    }
                    subject_results.append(result)
                except Exception as e:
                    print(f"Error: {e}")
                    print(f"subject_id: {subject_id}")
                    raise e

    return subject_results


def process_subject_wrapper(params: Tuple[Path, str]) -> List[Dict[str, Any]]:
    """
    Process a single subject's medication effect information.

    Args:
        params (Tuple[Path, str]): A tuple containing the config file path and subject ID.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing medication effect
            information for the subject.
    """
    config_file, subject_id = params
    return get_subject_medication_effect_info(
        config_file=config_file, subject_id=subject_id
    )


def compile_medication_effects(
    config_file: Path,
) -> None:
    """
    Compile medication effects for all subjects.
    """
    medication_effect_raw_data: List[Dict[str, Any]] = []

    subjects = data.get_all_subjects(config_file=config_file)

    # max_count = 25
    # subjects = random.sample(subjects, min(max_count, len(subjects)))

    num_processes = multiprocessing.cpu_count() // 4
    logger.info(f"Using {num_processes} processes for parallel computation.")
    params = [(config_file, subject_id) for subject_id in subjects]

    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for returned_result in pool.imap_unordered(process_subject_wrapper, params):
                medication_effect_raw_data.extend(returned_result)
                progress.update(task, advance=1)

    medication_effect_raw_data_df = pd.DataFrame(medication_effect_raw_data)

    int_cols = [
        "days_from_consent",
        "ap_equivalent_drug_dose_prescribed",
        "ap_equivalent_drug_dose_taken",
        "ad_equivalent_drug_dose_prescribed",
        "ad_equivalent_drug_dose_taken",
        "bd_equivalent_drug_dose_prescribed",
        "bd_equivalent_drug_dose_taken",
        "duration_prescribed_days",
        "current_use",
        "lifetime_use",
    ]

    for col in int_cols:
        medication_effect_raw_data_df[col] = (
            pd.to_numeric(medication_effect_raw_data_df[col], errors="coerce")
            .round(0)
            .astype("Int64")
        )

    # Cast date columns to datetime
    date_cols = ["date"]

    for col in date_cols:
        medication_effect_raw_data_df[col] = pd.to_datetime(
            medication_effect_raw_data_df[col], errors="coerce"
        )

    medication_effect_raw_data_df = medication_effect_raw_data_df.convert_dtypes()

    logger.info(
        f"Number of medication effect records: {len(medication_effect_raw_data_df)}"
    )
    logger.info(
        "Overwriting the medication_effect table in the database with the new data."
    )
    db.df_to_table(
        config_file=config_file,
        df=medication_effect_raw_data_df,
        table_name="medication_effect",
        schema="forms_derived",
        if_exists="replace",
    )

    return


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Compiling medication effects...")
    compile_medication_effects(config_file=config_file)
    logger.info("Done!")
