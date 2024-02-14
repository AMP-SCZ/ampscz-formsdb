#!/usr/bin/env python
"""
Fetches and consolidates various cognitive test's status and system status data
into a single dataframe and exports it to the database.
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
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from rich.logging import RichHandler

from formsqc import constants, data
from formsqc.helpers import db, dpdash, utils

MODULE_NAME = "formsqc.runners.compute.compute_cognition"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

required_variables: List[str] = []

required_system_status_variables: List[str] = [
    f"{test}_system_status" for test in constants.upenn_tests
]
required_manual_status_variables: List[str] = [
    f"{test}_status" for test in constants.upenn_tests
]

required_variables.extend(required_system_status_variables)
required_variables.extend(required_manual_status_variables)


def construct_init_queries(required_variables: List[str]) -> List[str]:
    """
    Constructs queries to drop and create the cognitive_summary and
    cognitive_data_availability tables.

    Args:
        required_variables (List[str]): List of required variables.

    Returns:
        List[str]: List of SQL queries.
    """
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
    """
    Initializes the database by dropping and creating the cognitive_summary and
    cognitive_data_availability tables.

    Args:
        config_file (Path): The path to the configuration file.
        required_variables (List[str]): List of required variables.

    Returns:
        None
    """
    init_queries = construct_init_queries(required_variables)

    db.execute_queries(
        config_file=config_file,
        queries=init_queries,
    )


def get_upenn_redcap_data(config_file: Path, subject_id: str) -> pd.DataFrame:
    """
    Fetches the UPenn form data for a subject from the database.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        pd.DataFrame: The UPenn form data.
    """
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
    """
    Get the system status for each test for a visit.

    Args:
        visit_data (pd.DataFrame): The visit's data.
        tests (List[str]): The list of tests.

    Returns:
        Dict[str, Any]: The system status for each test.
    """
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
            value = "Not Available"
        else:
            value = values[0]

        system_status[test] = value

    return system_status


def get_visit_status(
    visit_data: pd.DataFrame,
    tests: List[str],
) -> Dict[str, Any]:
    """
    Get the status for each test for a visit.

    Args:
        visit_data (pd.DataFrame): The visit's data.
        tests (List[str]): The list of tests.

    Returns:
        Dict[str, Any]: The status for each test.
    """
    status_dict: Dict[str, str] = {}

    # gets columns that contain 'system_status'
    all_cols: List[str] = visit_data.columns.to_list()
    status_cols: List[str] = [col for col in all_cols if "status" in col]

    # Skip 'system_status' columns
    status_cols = [col for col in status_cols if "system_status" not in col]

    for test in tests:
        test_cols: List[str] = [col for col in status_cols if test in col]
        values = visit_data[test_cols].values.tolist()

        # list of lists to list
        values = [item for sublist in values for item in sublist]

        # remove nan values
        values = [item for item in values if not pd.isna(item)]

        if len(values) == 0:
            value = "Not Available"
        else:
            value = values[0]

        status_dict[test] = value

    return status_dict


def get_visits_system_status(
    upenn_form: pd.DataFrame,
    visits: List[str],
    tests: List[str],
) -> Dict[str, Optional[Dict[str, str]]]:
    """
    Get the system status for each visit.

    Args:
        upenn_form (pd.DataFrame): The UPenn form data.
        visits (List[str]): The list of visits.
        tests (List[str]): The list of tests.

    Returns:
        Dict[str, Optional[Dict[str, str]]]: The system status for each visit.
    """
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


def get_visits_status(
    upenn_form: pd.DataFrame,
    visits: List[str],
    tests: List[str],
) -> Dict[str, Optional[Dict[str, str]]]:
    """
    Get the status for each visit.

    Args:
        upenn_form (pd.DataFrame): The UPenn form data.
        visits (List[str]): The list of visits.
        tests (List[str]): The list of tests.

    Returns:
        Dict[str, Optional[Dict[str, str]]]: The status for each visit.
    """
    visits_system_status: Dict[str, Optional[Dict[str, str]]] = {}

    for visit in visits:
        visit_data = upenn_form[upenn_form["event_name"].str.contains(visit)]
        visit_data = visit_data.dropna(axis=1, how="all")

        if visit_data.empty:
            visit_system_status = None
        else:
            visit_system_status = get_visit_status(visit_data, tests)

        visits_system_status[visit] = visit_system_status

    return visits_system_status


def generate_upenn_data_availability(system_status: Dict[str, Any]) -> pd.DataFrame:
    """
    Generates a dataframe of the data availability for each visit.

    data_availability_{visit} is True if data is available for the visit, False otherwise.
    data_availability_summary is a concatenated string of all visits with available data.
        e.g. "Visit 1, Visit 2, Visit 3"

    Args:
        system_status (Dict[str, Any]): The system status for each visit.

    Returns:
        pd.DataFrame: The data availability for each visit.
    """
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


def generate_upenn_data_summary(
    config_file: Path,
    subject_id: str,
    system_status: Dict[str, Any],
    manual_status: Dict[str, Any],
) -> pd.DataFrame:
    """
    Generates a dataframe of the data summary for each visit.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        system_status (Dict[str, Any]): The system status for each visit.
        manual_status (Dict[str, Any]): The manual status for each visit.

    Returns:
        pd.DataFrame: The data summary for each visit.
    """
    master_system_status_df = pd.DataFrame()
    master_status_df = pd.DataFrame()

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
        master_system_status_df = pd.concat([master_system_status_df, df])

    for event_name, status in manual_status.items():
        if status is None:
            continue

        df = pd.DataFrame(status, index=[event_name])

        # Append "_status" to each column name
        df = df.add_suffix("_status")

        df["event_name"] = event_name

        master_status_df = pd.concat([master_status_df, df])
    try:
        master_df = pd.merge(master_system_status_df, master_status_df, on="event_name")
    except KeyError as e:
        raise e
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
        print(e)

    return master_df


def availability_df_to_sql(df: pd.DataFrame, subject_id: str) -> List[str]:
    """
    Constructs SQL queries to insert the data availability dataframe into the database.

    Args:
        df (pd.DataFrame): The data availability dataframe.
        subject_id (str): The subject ID.

    Returns:
        List[str]: List of SQL queries.
    """
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
    """
    Constructs SQL queries to insert the data summary dataframe into the database.

    Args:
        df (pd.DataFrame): The data summary dataframe.
        subject_id (str): The subject ID.

    Returns:
        List[str]: List of SQL queries.
    """
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
    """
    Constructs SQL queries to insert the data availability and summary dataframes into the database.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        List[str]: List of SQL queries.
    """
    sql_queries: List[str] = []
    upenn_form = get_upenn_redcap_data(config_file=config_file, subject_id=subject_id)
    visits = constants.upenn_visit_order
    tests = constants.upenn_tests

    system_status = get_visits_system_status(
        upenn_form=upenn_form, visits=visits, tests=tests
    )

    manual_status = get_visits_status(upenn_form=upenn_form, visits=visits, tests=tests)

    df_availability = generate_upenn_data_availability(system_status)
    try:
        df_summary = generate_upenn_data_summary(
            config_file=config_file,
            subject_id=subject_id,
            system_status=system_status,
            manual_status=manual_status,
        )
    except KeyError:
        return []

    sql_queries.extend(
        availability_df_to_sql(df=df_availability, subject_id=subject_id)
    )
    sql_queries.extend(summary_df_to_sql(df=df_summary, subject_id=subject_id))

    return sql_queries


def process_data(config_file: Path) -> None:
    """
    Processes the data by fetching the data for each subject and constructing
    the SQL queries to insert the data into the database.

    Generates the data availability and summary dataframes and inserts them into
    the database.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
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
