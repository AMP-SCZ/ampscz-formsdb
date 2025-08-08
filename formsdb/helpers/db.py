"""
Helper functions for interacting with a PostgreSQL database.
"""

import json
import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import psycopg2
import pymongo
import pymongo.database as database
import sqlalchemy
from rich.console import Console
from sqlalchemy.exc import OperationalError
from psycopg2.errors import UndefinedTable

from formsdb.helpers import utils
from formsdb.helpers.config import config


def get_mongo_db(config_file: Path) -> database.Database:
    """
    Returns a MongoClient object.

    Returns:
        MongoClient: A MongoClient object.
    """
    config_params = config(config_file, "mongodb")
    host = config_params["host"]
    port = int(config_params["port"])
    db = config_params["database"]

    mongo_client = pymongo.MongoClient(host, port)

    db = mongo_client[db]
    return db


def check_if_subject_form_data_exists(
    config_file: Path, subject_id: str, source_mdate: datetime
) -> bool:
    """
    Checks if a subject's form data exists in the Postgres database and
    is up-to-date.

    A file is considered up-to-date if the m_date of the source file
    matches the hash stored in the database.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        source_mdate (str): The source modification date.

    Returns:
        bool: True if the subject's form data exists and is up-to-date, False otherwise.
    """
    query = f"""
    SELECT MAX(source_mdate) AS source_m_date
    FROM forms.forms
    WHERE subject_id = '{subject_id}'
    """

    source_m_date = fetch_record(config_file, query)

    if source_m_date is not None:
        source_m_date = datetime.strptime(source_m_date, "%Y-%m-%d %H:%M:%S")
        if source_mdate == source_m_date:
            return True

    return False


def check_if_subject_upenn_data_exists(
    config_file: Path,
    subject_id: str,
    source_m_date: datetime,
) -> bool:
    """
    Checks if a subject's UPenn data exists in the Postgres database and
    is up-to-date.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        source_m_date (datetime): The source modification date.
    """
    query = f"""
    SELECT MAX(source_mdate) AS source_m_date
    FROM forms.upenn_forms
    WHERE subject_id = '{subject_id}'
    """

    source_m_date = fetch_record(config_file, query)  # type: ignore

    if source_m_date is not None:
        source_m_date = datetime.strptime(source_m_date, "%Y-%m-%d %H:%M:%S")  # type: ignore
        if source_m_date == source_m_date:
            return True

    return False


def handle_null(query: str) -> str:
    """
    Replaces all occurrences of the string 'NULL' with the SQL NULL keyword in the given query.

    Args:
        query (str): The SQL query to modify.

    Returns:
        str: The modified SQL query with 'NULL' replaced with NULL.
    """
    query = query.replace("'NULL'", "NULL")

    return query


def handle_nan(query: str) -> str:
    """
    Replaces all occurrences of the string 'nan' with the SQL
    NULL keyword in the given query.

    Args:
        query (str): The SQL query to modify.

    Returns:
        str: The modified SQL query with 'nan' replaced with NULL.
    """
    query = query.replace("'nan'", "NULL")

    return query


def santize_string(string: str) -> str:
    """
    Sanitizes a string by escaping single quotes.

    Args:
        string (str): The string to sanitize.

    Returns:
        str: The sanitized string.
    """
    return string.replace("'", "''")


def sanitize_json(json_dict: dict) -> str:
    """
    Sanitizes a JSON object by replacing single quotes with double quotes.

    Args:
        json_dict (dict): The JSON object to sanitize.

    Returns:
        str: The sanitized JSON object.
    """
    for key, value in json_dict.items():
        if isinstance(value, str):
            json_dict[key] = santize_string(value)
        if isinstance(value, datetime):
            json_dict[key] = value.isoformat()
    try:
        json_str = json.dumps(json_dict)
    except TypeError:
        json_str = json.dumps(json_dict, default=str)

    return json_str


def execute_queries(
    config_file: Path,
    queries: list,
    show_commands=True,
    show_progress=False,
    silent=False,
) -> list:
    """
    Executes a list of SQL queries on a PostgreSQL database.

    Args:
        config_file_path (str): The path to the configuration file containing
            the connection parameters.
        queries (list): A list of SQL queries to execute.
        show_commands (bool, optional): Whether to display the executed SQL queries.
            Defaults to True.
        show_progress (bool, optional): Whether to display a progress bar.
            Defaults to False.
        silent (bool, optional): Whether to suppress output.
            Defaults to False.

    Returns:
        list: A list of tuples containing the results of the executed queries.
    """
    console = Console(color_system="standard")
    conn = None
    command = None
    output = []
    try:
        # read the connection parameters
        params = config(path=config_file, section="postgresql")
        # connect to the PostgreSQL server
        if show_commands:
            console.log("\nConnecting to the PostgreSQL database...")
            console.log(
                f"{params['host']}:{params['port']} {params['database']} ({params['user']})"
            )

        conn = psycopg2.connect(**params)  # type: ignore
        cur = conn.cursor()

        def execute_query(query: str):
            if show_commands:
                console.log("Executing Query: ")
                console.log(query, style="bold blue")
            cur.execute(query)
            try:
                output.append(cur.fetchall())
            except psycopg2.ProgrammingError:
                pass

        if show_progress:
            with utils.get_progress_bar() as progress:
                task = progress.add_task("Executing SQL queries...", total=len(queries))

                for command in queries:
                    progress.update(task, advance=1)
                    execute_query(command)

        else:
            for command in queries:
                execute_query(command)

        # close communication with the PostgreSQL database server
        cur.close()

        # commit the changes
        conn.commit()

        if not silent:
            console.log(f"Executed {len(queries)} SQL query(ies).")
    except (Exception, psycopg2.DatabaseError) as e:
        console.log("Error executing queries.", style="red")
        if command is not None:
            console.log(f"[red]For query: [bold]{command}[/bold][/red]")
        console.log("Error: " + str(e), style="red")
        raise e
    finally:
        if conn is not None:
            conn.close()

    return output


def get_db_connection(config_file: Path) -> sqlalchemy.engine.base.Engine:
    """
    Establishes a connection to the PostgreSQL database using the provided configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        sqlalchemy.engine.base.Engine: The database connection engine.
    """
    params = config(path=config_file, section="postgresql")
    db_uri: str = (
        "postgresql+psycopg2://"
        + params["user"]
        + ":"
        + params["password"]
        + "@"
        + params["host"]
        + ":"
        + params["port"]
        + "/"
        + params["database"]
    )
    engine = sqlalchemy.create_engine(db_uri, echo=False)

    return engine  # type: ignore


def execute_sql(
    config_file: Path, query: str, silence_logs: bool = True
) -> pd.DataFrame:
    """
    Executes a SQL query on a PostgreSQL database and returns the result as a
    pandas DataFrame.

    Args:
        config_file_path (str): The path to the configuration file containing the
            PostgreSQL database credentials.
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the result of the SQL query.
    """
    engine = get_db_connection(config_file=config_file)

    timeout = timedelta(seconds=2.5)

    while True:
        try:
            df = pd.read_sql(query, engine)
            break
        except OperationalError as e:
            if timeout > timedelta(seconds=300):
                raise e

            sleep_time = timeout.total_seconds() + random.uniform(
                1, timeout.total_seconds() / 2
            )
            if not silence_logs:
                logging.warning(f"OperationalError: Retrying after {sleep_time}s...")
            time.sleep(sleep_time)
            timeout = timeout * 2

            engine = get_db_connection(config_file=config_file)

    engine.dispose()

    return df


def fetch_record(config_file: Path, query: str) -> Optional[str]:
    """
    Fetches a single record from the database using the provided SQL query.

    Args:
        config_file_path (str): The path to the database configuration file.
        query (str): The SQL query to execute.

    Returns:
        Optional[str]: The value of the first column of the first row of the result set,
        or None if the result set is empty.
    """
    df = execute_sql(config_file=config_file, query=query)

    # Check if there is a row
    if df.shape[0] == 0:
        return None

    value = df.iloc[0, 0]

    if value is None:
        return None

    return str(value)


def df_to_table(
    config_file: Path,
    df: pd.DataFrame,
    table_name: str,
    schema: str = "public",
    if_exists: Literal["fail", "replace", "append"] = "replace",
) -> None:
    """
    Writes a pandas DataFrame to a table in a PostgreSQL database.

    Args:
        config_file (Path): The path to the configuration file.
        df (pd.DataFrame): The DataFrame to write to the database.
        table_name (str): The name of the table to write to.
        if_exists (Literal["fail", "replace", "append"], optional): What to do
            if the table already exists.
    """
    # If replace, TRUNCATE and append
    if if_exists == "replace":
        try:
            # Truncate the table before appending
            logging.info(f"Truncating table {schema}.{table_name} before appending data.")
            query = f"TRUNCATE TABLE {schema}.{table_name};"
            execute_queries(config_file, [query], silent=True)
            if_exists = "append"
        except UndefinedTable:
            logging.info(f"Table {schema}.{table_name} does not exist. Creating new table.")

    engine = get_db_connection(config_file=config_file)

    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
    engine.dispose()
