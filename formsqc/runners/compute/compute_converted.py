#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "ampscz-formsqc":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Any, Dict, List, Optional

from rich.logging import RichHandler
import pandas as pd
import numpy as np

from formsqc.helpers import db, utils
from formsqc import constants, data

MODULE_NAME = "formsqc_compute_converted"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def count_converted(config_file: Path) -> int:
    query = "SELECT COUNT(*) FROM subject_converted WHERE subject_converted = 'True';"
    converted_count_r = db.fetch_record(config_file=config_file, query=query)
    if converted_count_r is None:
        raise Exception("No converted status found in the database.")
    converted_count = int(converted_count_r)

    return converted_count


def check_if_converted(
    df: pd.DataFrame, visit_order: List[str], debug: bool = False
) -> Optional[str]:
    psychs_df = df[df["form_name"].str.contains("psychs_p9ac32_fu")]

    if psychs_df.shape[0] == 0:
        return None

    for visit in visit_order:
        visit_psychs_df = psychs_df[psychs_df["event_name"].str.contains(f"{visit}_")]
        if visit_psychs_df.shape[0] == 0:
            if debug:
                print(f"No psychs_p9ac32_fu form found for {visit}.")
            continue

        form_r = visit_psychs_df.iloc[0, :]
        form_data: Dict[str, Any] = form_r["form_data"]

        form_variables = list(form_data.keys())

        variable = "chrpsychs_fu_ac1_conv"
        if variable in form_variables:
            if form_data[variable] == 1:
                if debug:
                    print(f"Subject converted at {visit}.")
                return visit
        else:
            if debug:
                print(f"{variable} not found in {visit}.")

    return None


def compute_converted(
    config_file: Path, visit_order: List[str], debug: bool = False
) -> pd.DataFrame:
    visit_status_df = pd.DataFrame(
        columns=["subject_id", "converted", "converted_visit"]
    )
    logger.info("Computing if subjects got converted...")

    query = "SELECT COUNT(*) FROM subjects;"
    subject_count_r = db.fetch_record(config_file=config_file, query=query)
    if subject_count_r is None:
        raise Exception("No subjects found in the database.")
    subject_count = int(subject_count_r)

    logger.info(f"Found {subject_count} subjects.")

    query = "SELECT id FROM subjects ORDER BY id;"
    engine = db.get_db_connection(config_file=config_file)

    subject_ids_r = pd.read_sql(query, engine, chunksize=1)

    with utils.get_progress_bar() as progress:
        task = progress.add_task("[red]Processing...", total=subject_count)

        for row in subject_ids_r:
            subject_id = row["id"].values[0]

            progress.update(task, advance=1, description=f"Processing {subject_id}...")

            df = data.get_all_subject_forms(
                config_file=config_file, subject_id=subject_id
            )
            converted = check_if_converted(df=df, visit_order=visit_order, debug=debug)
            if converted is None:
                converted_visit = np.nan
                converted = False
            else:
                converted_visit = converted
                converted = True

            visit_status_df = pd.concat(
                [
                    visit_status_df,
                    pd.DataFrame(
                        {
                            "subject_id": [subject_id],
                            "converted": [converted],
                            "converted_visit": [converted_visit],
                        }
                    ),
                ]
            )

    logger.info(f"Done computing converted status for {subject_count} subjects.")

    return visit_status_df


def commit_converted_status_to_db(config_file: Path, df: pd.DataFrame) -> None:
    logger.info("Committing converted status to the database...")

    sql_queries: List[str] = []
    # Remove existing data
    sql_query = """
    DELETE FROM subject_converted;
    """
    sql_queries.append(sql_query)

    for idx, row in df.iterrows():
        subject_id = row["subject_id"]
        converted = row["converted"]
        converted_visit = row["converted_visit"]

        if pd.isna(converted_visit):
            converted_visit = "NULL"

        sql_query = f"""
        INSERT INTO subject_converted (subject_id, subject_converted, subject_converted_event)
        VALUES ('{subject_id}', '{converted}', '{converted_visit}');
        """
        sql_query = db.handle_null(sql_query)
        sql_queries.append(sql_query)

    logger.info("Removing existing data and committing new data...")
    db.execute_queries(
        config_file=config_file, queries=sql_queries, show_commands=False
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    converted_count = count_converted(config_file=config_file)
    logger.info(f"Found {converted_count} converted subjects.")

    converted_status_df = compute_converted(
        config_file, visit_order=constants.visit_order, debug=False
    )

    commit_converted_status_to_db(config_file, converted_status_df)

    new_converted_count = count_converted(config_file=config_file)
    logger.info(f"Found {new_converted_count} converted subjects after update.")
    logger.info(f"Added {new_converted_count - converted_count} converted subjects.")

    logger.info("Done!")
