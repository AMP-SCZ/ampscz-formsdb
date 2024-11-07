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
from typing import Any, Dict, List, Union, Optional

import pandas as pd
from rich.logging import RichHandler

from formsdb import constants
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


def process_subject(
    subject_path: Path,
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
        return {
            "subject_id": subject_id,
            "queries": [],
        }

    inclusionexclusion_criteria_review_data = pd.read_csv(
        inclusionexclusion_criteria_review_path, dtype=str
    )
    chrcrit_part = inclusionexclusion_criteria_review_data["chrcrit_part"].iloc[0]
    arm = f"arm_{chrcrit_part}"

    subject_drop_query = f"DELETE FROM forms WHERE subject_id = '{subject_id}'"
    insert_subject_query = export_subject(subject_id)
    subject_queries.append(subject_drop_query.strip())
    subject_queries.append(insert_subject_query.strip())
    subject_cohort: Optional[str] = None

    for form_name, suffix in constants.form_name_to_rpms_suffix.items():
        form_path = subject_surveys_root / f"{subject_id}_{suffix}"
        if not form_path.exists():
            # Form {form_name} not found for subject {subject_id}"
            continue

        form_data = pd.read_csv(form_path, dtype=str)

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

            subject_cohort = form_data["group"].iloc[0]
            if subject_cohort == "UHR":
                subject_cohort = "CHR"
            elif subject_cohort == "HealthyControl":
                subject_cohort = "HC"
            else:
                raise ValueError(f"Unknown cohort: {subject_cohort}")

        if form_name == "sociodemographics":
            if subject_cohort == "CHR":
                form_data["chrdemo_age_mos_chr"] = form_data["interview_age"]
            elif subject_cohort == "HC":
                form_data["chrdemo_age_mos_hc"] = form_data["interview_age"]
            else:
                raise ValueError(f"Unknown cohort: {subject_cohort}")

        if form_name == "coenrollment_form":
            # only insert first row
            form_data = form_data.head(1)

        for _, row in form_data.iterrows():
            visit = int(row["visit"])
            visit_redcap_event_name = constants.rpms_to_redcap_event[visit]

            visit_data = row.to_dict()
            form_metadata = get_form_visit_metadata(visit_data)

            # Remove blank columns
            slim_visit_data = {}
            for column in visit_data.keys():
                if pd.notna(visit_data[column]):
                    slim_visit_data[column] = visit_data[column]

            source_m_date = utils.get_file_mtime(form_path)

            insert_query = f"""
            INSERT INTO forms (subject_id, form_name, event_name,
                form_data, source_mdate,
                variables_with_data,
                variables_without_data,
                total_variables,
                percent_complete
            ) VALUES ('{subject_id}', '{form_name}', '{visit_redcap_event_name}_{arm}',
                '{db.sanitize_json(slim_visit_data)}', '{source_m_date}',
                {form_metadata['variables_with_data']},
                {form_metadata['variables_without_data']},
                {form_metadata['total_variables']},
                {form_metadata['percent_complete']}
            )
            """
            subject_queries.append(insert_query.strip())

    result = {
        "subject_id": subject_id,
        "queries": subject_queries,
    }

    return result


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

    subjects_glob = list(data_root.glob(f"{network_id}/PHOENIX/PROTECTED/*/raw/*"))

    queries: List[str] = []
    skipped_subjects: List[str] = []

    num_processes = multiprocessing.cpu_count() // 2
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(subjects_glob))
            for result in pool.imap_unordered(process_subject, subjects_glob):  # type: ignore
                subject_id = result["subject_id"]
                subject_queries = result["queries"]

                if len(subject_queries) == 0:
                    skipped_subjects.append(subject_id)  # type: ignore
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
