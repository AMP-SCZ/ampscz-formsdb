#!/usr/bin/env python
"""
Import RPMS CSVs into Postgres
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
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.imports.import_rpms_csvs"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def export_subject(subject_id: str) -> str:
    """
    Insert a subject into the subjects table.

    Args:
        subject_id (str): The subject id.

    Returns:
        str: The SQL query.
    """
    site_id = subject_id[:2]
    sql_query = f"""
    INSERT INTO subjects (id, site_id)
    VALUES ('{subject_id}', '{site_id}') ON CONFLICT DO NOTHING;
    """

    return sql_query


# Reference:
# https://github.com/AMP-SCZ/utility/blob/15a5ef5b49d1e081ee0a549375f78bb26160d958/rpms_to_redcap.py#L55C1-L71C21
def handle_datetime(time_value: str) -> datetime:
    """
    Handles different time formats from RPMS.

    This helps standardize the time formats to be used in the exported CSVs.

    Args:
        time_value (str): Time value to handle.

    Returns:
        datetime: Time value in datetime format.
    """
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
        raise ValueError(f"Unknown time format: {time_value}")

    return datetime_val


def get_form_visit_metadata(visit_data: Dict[str, str]) -> Dict[str, Any]:
    """
    Compute metadata for a form visit.

    Args:
        visit_data (Dict[str, str]): The visit data.

    Returns:
        Dict[str, Any]: The form metadata.
            - total_variables (int): The total number of variables.
            - variables_with_data (int): The number of variables with data.
            - variables_without_data (int): The number of variables without data.
            - percent_complete (float): The percentage of variables with data.
    """
    total_variables = len(visit_data.keys())
    variables_with_data: int = 0
    variables_without_data: int = 0

    ignored_columns = [
        "LastModifiedDate",
        "subjectkey",
        "interview_date",
        "interview_age",
        "gender",
        "visit",
    ]

    for column in ignored_columns:
        if column in visit_data:
            del visit_data[column]

    for _, value in visit_data.items():
        if pd.notna(value):
            if value == "-3" or value == "-9":
                variables_without_data += 1
            else:
                variables_with_data += 1
        else:
            variables_without_data += 1

    percent_complete = (variables_with_data / total_variables) * 100

    form_metadata = {
        "total_variables": total_variables,
        "variables_with_data": variables_with_data,
        "variables_without_data": variables_without_data,
        "percent_complete": percent_complete,
    }

    return form_metadata


# Referece:
# https://github.com/AMP-SCZ/utility/blob/15a5ef5b49d1e081ee0a549375f78bb26160d958/rpms_to_redcap.py#L152-L173
def rpms_to_redcap_entry_status_map(
    rpms_status: str,
) -> str:
    """
    RPMS policy
    status color  meaning
    0      Red    No data entered
    1      Orange Data partially entered
    2      Green  All data entered
    """

    status = int(rpms_status)
    if status == 0:
        return ""
    elif status == 1:
        return "0"
    elif status >= 2 and status <= 4:
        return "2"

    return ""


def get_subject_form_completion_variables(
    subject_id: str,
    config_file: Path,
    cohort: Optional[str] = None,
) -> List[str]:
    """
    Generates SQL queries with form completion variables for a subject.

    Args:
        subject_id (str): The subject id.
        config_file (Path): The config file.

    Returns:
        List[str]: The SQL queries.
    """
    if cohort is None:
        cohort = data.get_subject_cohort(
            subject_id=subject_id,
            config_file=config_file,
        )
    entry_status_df = data.get_all_rpms_entry_status(
        subject_id=subject_id, config_file=config_file
    )

    visits = entry_status_df["redcap_event_name"].unique().tolist()
    visits = sorted(visits)

    sql_queries: List[str] = []
    for visit in visits:
        event_name = visit
        visit_df = entry_status_df[entry_status_df["redcap_event_name"] == visit]

        if cohort == "HC":
            redcap_event_name = f"{event_name}_arm_2"
        elif cohort == "CHR":
            redcap_event_name = f"{event_name}_arm_1"
        else:
            raise ValueError(f"Invalid cohort: {cohort}")

        visit_data: Dict[str, int] = {}
        for _, row in visit_df.iterrows():
            redcap_form_name = row["redcap_form_name"]
            rpms_status = row["CompletionStatus"]
            redcap_status = rpms_to_redcap_entry_status_map(rpms_status)

            if redcap_status == "":
                continue

            if redcap_form_name is not None:
                redcap_variable = f"{redcap_form_name}_complete"
                visit_data[f"{redcap_variable}_rpms"] = int(rpms_status)
                visit_data[redcap_variable] = int(redcap_status)

        if len(visit_data) > 0:
            insert_query = f"""
            INSERT INTO forms.forms (subject_id, form_name, event_name,
                form_data, source_mdate,
                variables_with_data
            ) VALUES ('{subject_id}', 'uncategorized', '{redcap_event_name}',
                '{db.sanitize_json(visit_data)}', '{datetime.now().date()}',
                {len(visit_data)}
            );
            """
            sql_queries.append(insert_query)

    if len(sql_queries) == 0:
        logger.warning(
            f"No form completion variables found for subject {subject_id} "
            f"- number of visits: {len(visits)}"
        )

    return sql_queries


def process_subject(
    subject_path: Path,
    config_file: Path,
) -> Dict[str, Union[str, List[str]]]:
    """
    Generates SQL queries for a subject, to update the forms table.

    Args:
        subject_path (Path): The subject path.

    Returns:
        Dict[str, Union[str, List[str]]]: The SQL queries.
            - subject_id (str): The subject id.
            - queries (List[str]): The SQL queries.
    """
    subject_surveys_root = subject_path / "surveys"
    subject_queries: List[str] = []

    subject_id = subject_path.stem
    subject_surveys_root = subject_path / "surveys"

    # Determine cohort
    inclusionexclusion_criteria_review_path = (
        subject_surveys_root
        / f"{subject_id}_{constants.form_name_to_rpms_suffix['inclusionexclusion_criteria_review']}"
    )

    if not inclusionexclusion_criteria_review_path.exists():
        # inclusionexclusion_criteria_review form not found.
        # logger.warning(
        #     f"Inclusion/Exclusion Criteria Review form not found for subject {subject_id}"
        # )
        subject_cohort: Optional[str] = None
        arm: Optional[int] = None
    else:
        inclusionexclusion_criteria_review_data = pd.read_csv(
            inclusionexclusion_criteria_review_path, dtype=str, keep_default_na=False
        )
        chrcrit_part = int(inclusionexclusion_criteria_review_data["chrcrit_part"].iloc[0])
        if chrcrit_part == 1:
            subject_cohort = "CHR"
            arm = 1
        elif chrcrit_part == 2:
            subject_cohort = "HC"
            arm = 2
        else:
            raise ValueError(
                f"Unknown chrcrit_part value: {chrcrit_part} for subject {subject_id}"
            )

    for form_name, suffix in constants.form_name_to_rpms_suffix.items():
        form_path = subject_surveys_root / f"{subject_id}_{suffix}"
        if not form_path.exists():
            # Form {form_name} not found for subject {subject_id}"
            continue

        form_data = pd.read_csv(form_path, dtype=str, keep_default_na=False)
        # replace all empty strings with pd.NA
        form_data = form_data.replace("", pd.NA)

        if form_name == "informed_consent_run_sheet":
            # Only insert row with earlest 'chric_consent_date', format: DD/MM/YYYY 12:00:00 AM
            # drop all other rows
            form_data["chric_consent_date"] = pd.to_datetime(
                form_data["chric_consent_date"], format="%d/%m/%Y %I:%M:%S %p"
            )
            form_data = form_data.sort_values(by="chric_consent_date")
            form_data = form_data.head(1)

            # convert date to string
            form_data["chric_consent_date"] = form_data[
                "chric_consent_date"
            ].dt.strftime("%d/%m/%Y %I:%M:%S %p")

            if subject_cohort is None:
                subject_cohort = form_data["group"].iloc[0]
                if subject_cohort == "UHR":
                    subject_cohort = "CHR"
                    arm = 1
                elif subject_cohort == "HealthyControl":
                    subject_cohort = "HC"
                    arm = 2
                else:
                    raise ValueError(f"{form_name} - Unknown cohort: {subject_cohort}")

            form_data["chric_record_id"] = subject_id

        if form_name == "sociodemographics":
            if subject_cohort == "CHR":
                form_data["chrdemo_age_mos_chr"] = form_data["interview_age"]
            elif subject_cohort == "HC":
                form_data["chrdemo_age_mos_hc"] = form_data["interview_age"]
            else:
                raise ValueError(f"{form_name} - Unknown cohort: {subject_cohort}")

        if form_name == "coenrollment_form":
            # only insert first row
            form_data = form_data.head(1)

        if form_name == "current_pharmaceutical_treatment_floating_med_125":
            form_data["chrpharm_date_mod"]  = form_data["LastModifiedDate"]
        elif form_name == "current_pharmaceutical_treatment_floating_med_2650":
            form_data["chrpharm_date_mod_2"] = form_data["LastModifiedDate"]

        for _, row in form_data.iterrows():
            visit = int(row["visit"])
            visit_redcap_event_name = constants.rpms_to_redcap_event[visit]

            visit_data = row.to_dict()
            form_metadata = get_form_visit_metadata(visit_data)

            # Remove blank columns
            slim_visit_data = {}
            for column in visit_data.keys():
                value: str = visit_data[column]
                if pd.notna(value):
                    value = str(value).strip()
                    if value.isdigit() or (
                        value.startswith("-") and value[1:].isdigit()
                    ):
                        value = int(value)  # type: ignore
                    elif value.replace(".", "", 1).isdigit() or (
                        value.startswith("-")
                        and value[1:].replace(".", "", 1).isdigit()
                    ):
                        value = float(value)  # type: ignore
                    else:
                        try:
                            value = handle_datetime(value)  # type: ignore
                        except ValueError:
                            pass
                    slim_visit_data[column] = value

            source_m_date = utils.get_file_mtime(form_path)

            insert_query = f"""
            INSERT INTO forms.forms (subject_id, form_name, event_name,
                form_data, source_mdate,
                variables_with_data,
                variables_without_data,
                total_variables,
                percent_complete
            ) VALUES ('{subject_id}', '{form_name}', '{visit_redcap_event_name}_arm_{arm}',
                '{db.sanitize_json(slim_visit_data)}', '{source_m_date}',
                {form_metadata['variables_with_data']},
                {form_metadata['variables_without_data']},
                {form_metadata['total_variables']},
                {form_metadata['percent_complete']}
            )
            """
            subject_queries.append(insert_query.strip())
    try:
        uncategorized_queries = get_subject_form_completion_variables(
            subject_id=subject_id, config_file=config_file, cohort=subject_cohort
        )
    except ValueError as e:
        logger.error(f"Error linking form completion variables for subject {subject_id}: {e}")
        uncategorized_queries = []

    subject_queries.extend(uncategorized_queries)

    if len(subject_queries) > 0:
        insert_subject_query = export_subject(subject_id)
        subject_queries.insert(0, insert_subject_query.strip())
    else:
        drop_subject_query = f"""
        DELETE FROM subjects WHERE id = '{subject_id}';
        """
        subject_queries.insert(0, drop_subject_query.strip())

    subject_drop_query = f"""
    DELETE FROM forms.forms WHERE subject_id = '{subject_id}'
    """
    subject_queries.insert(0, subject_drop_query.strip())

    result = {
        "subject_id": subject_id,
        "queries": subject_queries,
    }

    return result


def process_subject_wrapper(
    params: Tuple[Path, Path]
) -> Dict[str, Union[str, List[str]]]:
    """
    Wrapper function for process_subject.

    Args:
        params (Tuple[Path, Path]): The parameters.

    Returns:
        Dict[str, Union[str, List[str]]]: The result.
    """
    subject_path, config_file = params
    return process_subject(subject_path=subject_path, config_file=config_file)


def import_forms_by_network(
    config_file: Path,
) -> List[str]:
    """
    Generates SQL queries for all subjects in PRESCIENT network.

    Args:
        config_file (Path): The config file.

    Returns:
        List[str]: The SQL queries.
    """
    config_params = utils.config(config_file, "general")
    data_root = Path(config_params["data_root"])

    network_id = "Prescient"
    search_path = f"{network_id}/PHOENIX/PROTECTED/*/raw/*"
    logger.info(f"Searching for subjects in {data_root}/{search_path}")
    subjects_glob = list(data_root.glob(search_path))

    queries: List[str] = []
    skipped_subjects: List[str] = []

    log_frequency = max(1, len(subjects_glob) // 10)

    num_processes = 8
    params = [(subject_path, config_file) for subject_path in subjects_glob]

    completed_subjects: List[str] = []
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(subjects_glob))
            for result in pool.imap_unordered(process_subject_wrapper, params):
                subject_id = result["subject_id"]
                subject_queries = result["queries"]

                completed_subjects.append(subject_id)  # type: ignore
                if len(completed_subjects) % log_frequency == 0:
                    logger.info(
                        f"Processed {len(completed_subjects)}/{len(subjects_glob)} "
                        f"subjects ({(len(completed_subjects)/len(subjects_glob))*100:.1f}%)"
                    )

                if len(subject_queries) == 0:
                    skipped_subjects.append(subject_id)  # type: ignore
                    progress.update(task, advance=1)
                    continue

                queries.extend(subject_queries)
                progress.update(task, advance=1)

    if len(skipped_subjects) > 0:
        logger.info(f"Skipped {len(skipped_subjects)} subjects")
        logger.debug(f"Skipped subjects: {skipped_subjects}")
    return queries


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    config_params = utils.config(config_file, "general")
    data_root = Path(config_params["data_root"])

    logger.info("Importing RPMS CSVs into Postgres")
    import_queries = import_forms_by_network(config_file=config_file)
    db.execute_queries(
        config_file=config_file,
        queries=import_queries,
        show_progress=True,
        show_commands=False,
    )

    logger.info("Done")
