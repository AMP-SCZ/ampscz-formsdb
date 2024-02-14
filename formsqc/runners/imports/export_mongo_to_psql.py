#!/usr/bin/env python
"""
Export forms data from MongoDB to PostgreSQL.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "ampscz-formsqc":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from datetime import datetime
from typing import Any, Dict, List, Set

from rich.logging import RichHandler

from formsqc.helpers import db, utils
from formsqc.models import forms as forms_model

MODULE_NAME = "formsqc_psql_importer"

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


def export_forms(config_file: Path):
    """
    Export forms data from MongoDB to PostgreSQL.
    """
    mongodb = db.get_mongo_db(config_file)
    forms = mongodb["forms"]

    forms_data: List[forms_model.Forms] = []
    sql_queries: List[str] = []

    f_count = 0
    for form in forms.find():
        form_dict: Dict[str, Any] = form
        subject_id = form_dict["_id"]
        source_m_date = form_dict["_source_mdate"]

        sql_queries.append(export_subject(subject_id))

        for key, value in form_dict.items():
            if isinstance(value, dict):
                form_name = key
                form_data_across_events = value

                for event, value in form_data_across_events.items():
                    if isinstance(value, dict):
                        event_form_data = value
                        form_metadata = event_form_data.pop("metadata")

                        for key, value in event_form_data.items():
                            if isinstance(value, datetime):
                                # convert datetime to isoformat
                                event_form_data[key] = value.isoformat()

                        forms_data.append(
                            forms_model.Forms(
                                subject_id,
                                form_name,
                                event,
                                event_form_data,
                                source_m_date,
                                form_metadata,
                            )
                        )
                        f_count += 1

    logger.info(f"Total forms: {f_count}")

    logger.info("Constructing SQL queries...")
    subjects: Set[str] = set()
    with utils.get_progress_bar() as progress:
        form_process = progress.add_task(
            "[red]Processing forms...", total=len(forms_data)
        )
        for form in forms_data:
            progress.update(
                form_process,
                advance=1,
                description=f"Constructing queries for [{form.subject_id}]...",
            )
            form_metadata = form.metadata

            variables_with_data = form_metadata["variables_with_data"]
            try:
                variables_without_data = form_metadata["variables_without_data"]
                total_variables = form_metadata["total_variables"]
                percent_complete = form_metadata["percent_data_available"]
            except KeyError:
                variables_without_data = "NULL"
                total_variables = "NULL"
                percent_complete = "NULL"

            insert_query = f"""
                INSERT INTO forms (subject_id, form_name, event_name,
                    form_data, source_mdate, variables_with_data,
                    variables_without_data, total_variables, percent_complete)
                VALUES ('{form.subject_id}', '{form.form_name}', '{form.event_name}',
                    '{db.sanitize_json(form.form_data)}', '{form.source_m_date}', {variables_with_data},
                    {variables_without_data}, {total_variables}, {percent_complete})
                """

            subjects.add(form.subject_id)
            sql_queries.append(db.handle_null(insert_query))

    delete_queries: List[str] = []
    for subject in subjects:
        delete_queries.append(f"DELETE FROM forms WHERE subject_id = '{subject}';")

    db.execute_queries(
        config_file=config_file,
        queries=delete_queries + sql_queries,
        show_commands=False,
        show_progress=True,
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    logger.info("Exporting forms...")
    export_forms(config_file)

    logger.info("Done!")
