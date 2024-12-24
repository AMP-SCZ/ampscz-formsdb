#!/usr/bin/env python
"""
Export UPENN forms data from MongoDB to PostgreSQL.
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
from datetime import datetime
from typing import Any, Dict, List, Set

from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import db, utils
from formsdb.models import upenn_forms as upenn_forms_model

MODULE_NAME = "formsdb.runners.imports.export_upenn_mongo_to_psql"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def export_forms(config_file: Path) -> None:
    """
    Export UPENN forms data from MongoDB to PostgreSQL.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        None
    """
    mongodb = db.get_mongo_db(config_file)

    forms_data: List[upenn_forms_model.UpennForms] = []
    sql_queries: List[str] = []

    for collection in ["upenn_nda"]:
        logger.info(f"Exporting {collection} forms...")
        forms = mongodb[collection]
        f_count = 0
        for form in forms.find():
            form_dict: Dict[str, Any] = form
            subject_id = form_dict["_id"]
            source_m_date = form_dict["_source_mdate"]

            for key, value in form_dict.items():
                if isinstance(value, dict):
                    event_name = key
                    event_form_data = value

                    if collection == "upenn":
                        for event_type, value in event_form_data.items():
                            if isinstance(value, dict):
                                event_type_form_data = value

                                for key, value in event_type_form_data.items():
                                    if isinstance(value, datetime):
                                        # convert datetime to isoformat
                                        event_type_form_data[key] = value.isoformat()

                                forms_data.append(
                                    upenn_forms_model.UpennForms(
                                        subject_id=subject_id,
                                        event_name=event_name,
                                        event_type=event_type,
                                        form_data=event_type_form_data,
                                        source_m_date=source_m_date,
                                    )
                                )
                                f_count += 1
                    elif collection == "upenn_nda":
                        event_type = "nda"
                        for key, value in event_form_data.items():
                            if isinstance(value, datetime):
                                # convert datetime to isoformat
                                event_form_data[key] = value.isoformat()

                        forms_data.append(
                            upenn_forms_model.UpennForms(
                                subject_id=subject_id,
                                event_name=event_name,
                                event_type=event_type,
                                form_data=event_form_data,
                                source_m_date=source_m_date,
                            )
                        )
                        f_count += 1
        logger.info(f"Total forms: {f_count}")

    logger.info("Constructing SQL queries...")
    purged_subjects: Set[str] = set()

    with utils.get_progress_bar() as progress:
        task = progress.add_task("Constructing SQL queries...", total=len(forms_data))
        for form in forms_data:
            if not data.check_if_subject_exists(
                config_file=config_file, subject_id=form.subject_id
            ):
                logger.warning(
                    f"Subject {form.subject_id} does not exist in the database, skipping..."
                )
                continue

            if form.subject_id not in purged_subjects:
                sql_query = f"""DELETE FROM upenn_forms WHERE subject_id = '{form.subject_id}';"""
                sql_queries.append(sql_query)
                purged_subjects.add(form.subject_id)

            sql_query = f"""
                INSERT INTO forms.upenn_forms (subject_id, event_name, event_type,
                    form_data, source_mdate)
                VALUES ('{form.subject_id}', '{form.event_name}', '{form.event_type}',
                    '{db.sanitize_json(form.form_data)}', '{form.source_m_date}');
                """

            sql_queries.append(db.handle_null(sql_query))
            progress.update(task, advance=1)

    db.execute_queries(
        config_file=config_file,
        queries=sql_queries,
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
