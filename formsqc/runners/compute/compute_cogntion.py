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
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from rich.logging import RichHandler

from formsqc.helpers import db, utils, dpdash
from formsqc import constants, data

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

required_variables: List[str] = constants.upenn_tests
# Append "_system_status" to each variable
required_variables = [f"{variable}_system_status" for variable in required_variables]


def construct_init_queries(required_variables: List[str]) -> List[str]:
    drop_summary_table_query = """
        DROP TABLE IF EXISTS cognitive_summary;
    """
    drop_available_table_query = """
        DROP TABLE IF EXISTS cognitive_data_availability;
    """

    create_summary_table_query = f"""
        CREATE TABLE cognitive_summary (
            subject_id TEXT NOT NULL REFERENCES subjects(id),
            event_name TEXT NOT NULL,
            day INT NOT NULL,
            weekday INT,
            {", ".join([f"{variable} TEXT" for variable in required_variables])},
            PRIMARY KEY (subject_id, event_name)
        );
    """
    create_available_table_query = f"""
        CREATE TABLE cognitive_data_availability (
            subject_id TEXT NOT NULL REFERENCES subjects(id),
            {", ".join([f"data_availability_{variable} BOOLEAN" for variable in constants.upenn_visit_order])},
            data_availability_summary TEXT,
            PRIMARY KEY (subject_id)
        );
    """

    return [
        drop_summary_table_query,
        drop_available_table_query,
        create_summary_table_query,
        create_available_table_query,
    ]


def init_db(config_file: Path, required_variables: List[str]) -> None:
    init_queries = construct_init_queries(required_variables)

    db.execute_queries(
        config_file=config_file,
        queries=init_queries,
    )


def get_upenn_redcap_data(config_file: Path, subject_id: str) -> pd.DataFrame:
    query = f"""
    SELECT subject_id, event_name, event_type, form_data FROM upenn_forms
        WHERE subject_id = '{subject_id}';
    """

    upenn_form = db.execute_sql(config_file=config_file, query=query)
    # Explode the form data into a dataframe
    upenn_form = pd.concat(
        [
            upenn_form.drop("form_data", axis=1),
            pd.json_normalize(upenn_form["form_data"]),  # type: ignore
        ],
        axis=1,
    )

    return upenn_form


def get_visit_system_status(
    visit_data: pd.DataFrame,
    tests: List[str],
) -> Dict[str, Any]:
    system_status: Dict[str, str] = {}

    # gets columns that contain 'system_status'
    all_cols: List[str] = visit_data.columns.to_list()
    status_cols: List[str] = [col for col in all_cols if "system_status" in col]

    for test in tests:
        test_cols: List[str] = [col for col in status_cols if test in col]
        values = visit_data[test_cols].values.tolist()

        # list of lists to list
        values = [item for sublist in values for item in sublist]

        # remove nan values
        values = [item for item in values if not pd.isna(item)]

        if len(values) == 0:
            value = "Not Administered"
        else:
            value = values[0]

        system_status[test] = value

    return system_status


def get_visits_system_status(
    upenn_form: pd.DataFrame,
    visits: List[str],
    tests: List[str],
) -> Dict[str, Optional[Dict[str, str]]]:
    visits_system_status: Dict[str, Optional[Dict[str, str]]] = {}

    for visit in visits:
        visit_data = upenn_form[upenn_form["event_name"].str.contains(visit)]
        visit_data = visit_data.dropna(axis=1, how="all")

        if visit_data.empty:
            visit_system_status = None
        else:
            visit_system_status = get_visit_system_status(visit_data, tests)

        visits_system_status[visit] = visit_system_status

    return visits_system_status


def generate_upenn_data_availability(system_status: Dict[str, Any]) -> pd.DataFrame:
    visits = system_status.keys()
    data_availability: Dict[str, Union[bool, str]] = {}

    summary = ""

    for visit in visits:
        if system_status[visit] is None:
            data_availability[f"data_availability_{visit}"] = False
        else:
            data_availability[f"data_availability_{visit}"] = True
            summary = f"{summary}{visit}, "

    data_availability["data_availability_summary"] = summary[:-2]

    return pd.DataFrame(data_availability, index=[0])


def genertate_upenn_data_summary(
    config_file: Path, subject_id: str, system_status: Dict[str, Any]
) -> pd.DataFrame:
    master_df = pd.DataFrame()

    for event_name, status in system_status.items():
        if status is None:
            continue

        df = pd.DataFrame(status, index=[event_name])

        # Append "_system_status" to each column name
        df = df.add_suffix("_system_status")

        df["event_name"] = event_name

        df["day"] = data.get_upenn_days_since_consent(
            config_file=config_file, subject_id=subject_id, event_name=event_name
        )
        event_day = data.get_upenn_event_date(
            config_file=config_file, subject_id=subject_id, event_name=event_name
        )
        df["weekday"] = dpdash.get_week_day(event_day)
        master_df = pd.concat([master_df, df])

    master_df = master_df.reset_index(drop=True)

    # Sort columns alphabetically
    master_df = master_df.reindex(sorted(master_df.columns), axis=1)

    # move event_name to the front
    try:
        cols = master_df.columns.tolist()
        cols.remove("event_name")
        cols = ["event_name"] + cols
        master_df = master_df[cols]
    except ValueError as e:
        logger.warning(f"{subject_id} - {e}")

    return master_df


def availability_df_to_sql(df: pd.DataFrame, subject_id: str) -> List[str]:
    # df = make_df_dpdash_ready(df=df, subject_id=subject_id)
    sql_queries: List[str] = []

    sql_queries: List[str] = []
    for _, row in df.iterrows():
        query = f"""
            INSERT INTO cognitive_data_availability (
                subject_id, {", ".join(row.index.tolist())})
            VALUES ('{subject_id}', {", ".join([f"'{value}'" for value in row.values.tolist()])});
        """

        sql_queries.append(query)

    return sql_queries


def summary_df_to_sql(df: pd.DataFrame, subject_id: str) -> List[str]:
    df = data.make_df_dpdash_ready(df=df, subject_id=subject_id)
    sql_queries: List[str] = []

    for _, row in df.iterrows():
        event_name = row["event_name"]
        day = row["day"]
        weekday = row["weekday"]

        query = f"""
            INSERT INTO cognitive_summary (
                subject_id, event_name, day, weekday,
                {", ".join(required_variables)})
            VALUES ('{subject_id}', '{event_name}', {day}, {weekday},
                {", ".join([f"'{row[variable]}'" for variable in required_variables])});
        """

        sql_queries.append(query)

    return sql_queries


def construct_queries(config_file: Path, subject_id: str) -> List[str]:
    sql_queries: List[str] = []
    upenn_form = get_upenn_redcap_data(config_file=config_file, subject_id=subject_id)
    visits = constants.upenn_visit_order
    tests = constants.upenn_tests

    system_status = get_visits_system_status(
        upenn_form=upenn_form, visits=visits, tests=tests
    )

    df_availability = generate_upenn_data_availability(system_status)
    df_summary = genertate_upenn_data_summary(
        config_file=config_file, subject_id=subject_id, system_status=system_status
    )

    sql_queries.extend(
        availability_df_to_sql(df=df_availability, subject_id=subject_id)
    )
    sql_queries.extend(summary_df_to_sql(df=df_summary, subject_id=subject_id))

    return sql_queries


def process_data(config_file: Path) -> None:
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
                )
                if len(queries) < 1:
                    logger.warning(
                        f"No data found for subject {subject_id}, skipping..."
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
    init_db(config_file=config_file, required_variables=required_variables)

    logger.info("Processing data...")
    process_data(config_file=config_file)

    logger.info("Done.")
