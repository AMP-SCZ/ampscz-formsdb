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
from typing import Any, Dict, List

from rich.logging import RichHandler

from formsqc import data
from formsqc.helpers import db, utils
from formsqc.models import upenn_forms as upenn_forms_model

MODULE_NAME = "formsqc_upenn_psql_importer"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def export_forms(config_file: Path):
    """
    Export UPENN forms data from MongoDB to PostgreSQL.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        None
    """
    mongodb = db.get_mongo_db(config_file)
    forms = mongodb["upenn"]

    forms_data: List[upenn_forms_model.UpennForms] = []
    sql_queries: List[str] = []

    f_count = 0
    for form in forms.find():
        form_dict: Dict[str, Any] = form
        subject_id = form_dict["_id"]
        source_m_date = form_dict["_source_mdate"]

        for key, value in form_dict.items():
            if isinstance(value, dict):
                event_name = key
                event_form_data = value

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

    logger.info(f"Total forms: {f_count}")

    logger.info("Constructing SQL queries...")
    for form in forms_data:
        if not data.check_if_subject_exists(
            config_file=config_file, subject_id=form.subject_id
        ):
            logger.warning(f"Subject {form.subject_id} already exists. Skipping...")
            continue

        sql_query = (
            f"""DELETE FROM upenn_forms WHERE subject_id = '{form.subject_id}';"""
        )

        if sql_query not in sql_queries:
            sql_queries.append(sql_query)

        sql_query = f"""
            INSERT INTO upenn_forms (subject_id, event_name, event_type,
                form_data, source_mdate)
            VALUES ('{form.subject_id}', '{form.event_name}', '{form.event_type}',
                '{db.sanitize_json(form.form_data)}', '{form.source_m_date}');
            """

        sql_queries.append(db.handle_null(sql_query))

    db.execute_queries(
        config_file=config_file, queries=sql_queries, show_commands=False
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
