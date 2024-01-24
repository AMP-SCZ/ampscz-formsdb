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
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from rich.logging import RichHandler

from formsqc.helpers import db, utils
from formsqc import data

MODULE_NAME = "formsqc_compute_cognitive"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


required_variables: List[str] = [
    "mpract_valid_code",
    "spcptn90_valid_code",
    "er40_d_valid_code",
    "sfnb2_valid_code",
    "digsym_valid_code",
    "svolt_a_valid_code",
    "sctap_valid_code",
]

dp_dash_required_cols = [
    # "reftime",
    "day",
    # "timeofday",
    "weekday",
]


def init_db(config_file: Path) -> None:
    drop_table_query = """
        DROP TABLE IF EXISTS cognitive_data;
    """

    create_table_query = f"""
        CREATE TABLE cognitive_data (
            subject_id VARCHAR(25) NOT NULL,
            event_name VARCHAR(255) NOT NULL,
            day INT NOT NULL,
            weekday INT,
            {", ".join([f"{variable} VARCHAR(8)" for variable in required_variables])},
            PRIMARY KEY (subject_id, event_name)
        );
    """

    db.execute_queries(config_file, [drop_table_query, create_table_query])


def get_upenn_event_date(
    config_file: Path, subject_id: str, event_name: str
) -> datetime:
    query = f"""
    SELECT form_data ->> 'session_date' as session_date
    FROM upenn_forms
    WHERE subject_id = '{subject_id}' AND
        event_name LIKE '%%{event_name}%%' AND
        form_data ? 'session_date';
    """

    date = db.fetch_record(config_file=config_file, query=query)

    if date is None:
        raise Exception("No event date found in the database.")
    date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")

    return date


def get_days_since_consent(config_file: Path, subject_id: str, event_name: str) -> int:
    consent_date = data.get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )
    event_date = get_upenn_event_date(
        config_file=config_file, subject_id=subject_id, event_name=event_name
    )

    return (event_date - consent_date).days + 1


def fetch_required_variables(
    config_file: Path, subject_id: str, required_variables: List[str]
) -> pd.DataFrame:
    variables_df = pd.DataFrame(columns=required_variables)

    for required_variable in required_variables:
        sql_query = f"""
            SELECT subject_id, event_name, event_type, form_data ->> '{required_variable}' as {required_variable}
            FROM upenn_forms
            WHERE subject_id = '{subject_id}';
            """

        df = db.execute_sql(config_file=config_file, query=sql_query)
        if df.shape[0] == 0:
            continue

        df.dropna(inplace=True)

        for idx, row in df.iterrows():
            event = row["event_name"]
            value = row[required_variable]

            # print(event, required_variable, value)
            # Add this value to the dataframe
            variables_df.loc[event, required_variable] = value

    return variables_df


def get_week_day(datetime: datetime) -> int:
    # Return the day of the week as an integer, where Saturday is 1 and Friday is 7.
    return datetime.isoweekday()


def construct_queries(
    config_file: Path, subject_id: str, required_variables: List[str]
) -> List[str]:
    cols = dp_dash_required_cols + ["event_name", "mdate"] + required_variables
    vals: Dict[str, Any] = {}

    dp_dash_df = pd.DataFrame(columns=cols)
    variables_df = fetch_required_variables(
        config_file=config_file,
        subject_id=subject_id,
        required_variables=required_variables,
    )

    for index, row in variables_df.iterrows():
        event_name: str = str(index)
        vals["event_name"] = event_name

        vals["day"] = get_days_since_consent(
            config_file=config_file, subject_id=subject_id, event_name=event_name
        )

        event_day = get_upenn_event_date(
            config_file=config_file, subject_id=subject_id, event_name=event_name
        )
        vals["weekday"] = get_week_day(event_day)

        for required_variable in required_variables:
            value = row[required_variable]
            vals[required_variable] = value

        for key, value in vals.items():
            if key not in cols:
                continue
            dp_dash_df.loc[event_name, key] = value

    dp_dash_df.reset_index(inplace=True)
    sql_queries: List[str] = []

    for index, row in dp_dash_df.iterrows():
        event_name = row["event_name"]
        day = row["day"]
        weekday = row["weekday"]

        sql_query = f"""
            INSERT INTO cognitive_data (
                subject_id, event_name, day, weekday,
                {", ".join(required_variables)})
            VALUES ('{subject_id}', '{event_name}', {day}, {weekday},
                {", ".join([f"'{row[variable]}'" for variable in required_variables])});
        """
        sql_query = db.handle_nan(sql_query)

        sql_queries.append(sql_query)

    return sql_queries


def export_data(config_file: Path) -> None:
    subject_query = """
        SELECT DISTINCT subject_id FROM upenn_forms;
    """

    subject_id_df = db.execute_sql(config_file, subject_query)
    subject_ids = subject_id_df["subject_id"].tolist()

    subjects_count = len(subject_ids)
    logger.info(f"Exporting data for {subjects_count} subjects...")

    sql_queries: List[str] = []
    with utils.get_progress_bar() as progress:
        task = progress.add_task("Processing...", total=subjects_count)

        for subject_id in subject_ids:
            progress.update(task, advance=1, description=f"Processing {subject_id}...")

            try:
                queries = construct_queries(
                    config_file=config_file,
                    subject_id=subject_id,
                    required_variables=required_variables,
                )
                if len(queries) < 1:
                    logger.warning(
                        f"No required variables found for subject {subject_id}, skipping..."
                    )

                sql_queries.extend(queries)
            except data.NoSubjectConsentDateException:
                logger.warning(
                    f"No consent date found for subject {subject_id}, skipping..."
                )
                continue

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

    logger.info("Initializing database...")
    init_db(config_file)

    logger.info("Exporting data...")
    export_data(config_file)

    logger.info("Done.")
