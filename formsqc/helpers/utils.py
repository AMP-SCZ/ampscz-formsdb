import logging
from datetime import datetime
from pathlib import Path
from typing import Union

from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TaskProgressColumn,
    MofNCompleteColumn,
    TimeRemainingColumn,
)
import pandas as pd

from formsqc.helpers import cli, db
from formsqc.helpers.config import config

_console = Console(color_system="standard")


def get_progress_bar() -> Progress:
    """
    Returns a rich Progress object with standard columns.

    Returns:
        Progress: A rich Progress object with standard columns.
    """
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )


def get_console() -> Console:
    """
    Returns a Console object with standard color system.

    Returns:
        Console: A Console object with standard color system.
    """
    return _console


def get_curent_datetime(iso: bool = False) -> Union[str, datetime]:
    """
    Returns the current date and time in ISO format.

    Returns:
        str: The current date and time in the ISO format.
    """

    dt = datetime.now()

    if iso:
        return dt.isoformat()

    return dt


def get_file_mtime(file: Path) -> datetime:
    """
    Returns the modification time of the file.

    Args:
        file (Path): The path to the file.

    Returns:
        str: The modification time of the file.
    """
    return datetime.fromtimestamp(file.stat().st_mtime)


def is_date(date: str) -> bool:
    """
    Checks if the date is in the format YYYY-MM-DD.

    Args:
        date (str): The date to check.

    Returns:
        bool: True if the date is in the format YYYY-MM-DD, False otherwise.
    """
    try:
        datetime.strptime(date, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def is_time(time: str) -> bool:
    """
    Checks if the time is in the format HH:MM.

    Args:
        time (str): The time to check.

    Returns:
        bool: True if the time is in the format HH:MM, False otherwise.
    """
    try:
        datetime.strptime(time, "%H:%M")
        return True
    except ValueError:
        return False


def is_datetime(date: str) -> bool:
    """
    Checks if the date is in the format YYYY-MM-DD HH:MM.

    Args:
        date (str): The date to check.

    Returns:
        bool: True if the date is in the format YYYY-MM-DD HH:MM, False otherwise.
    """
    try:
        datetime.strptime(date, "%Y-%m-%d %H:%M")
        return True
    except ValueError:
        return False


def validate_date(date: str) -> bool:
    """
    Validates a date string.

    Args:
        date (str): The date string to validate.

    Returns:
        bool: True if the date string is valid, False otherwise.
    """
    try:
        pd.to_datetime(date)
        return True
    except ValueError:
        return False


def configure_logging(config_file: Path, module_name: str, logger: logging.Logger):
    """
    Configures logging for a given module using the specified configuration file.

    Args:
        config_file (str): The path to the configuration file.
        module_name (str): The name of the module to configure logging for.
        logger (logging.Logger): The logger object to use for logging.

    Returns:
        None
    """
    log_params = config(config_file, "logging")
    log_file = log_params[module_name]

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  - %(process)d - %(name)s - %(levelname)s - %(message)s"
        )
    )

    logging.getLogger().addHandler(file_handler)

    logger.info(f"Logging to {log_file}")


def get_config_file_path() -> Path:
    """
    Returns the path to the config file.

    Returns:
        str: The path to the config file.

    Raises:
        ConfigFileNotFoundExeption: If the config file is not found.
    """
    repo_root = cli.get_repo_root()
    config_file_path = repo_root + "/config.ini"

    # Check if config_file_path exists
    if not Path(config_file_path).is_file():
        raise FileNotFoundError(f"Config file not found at {config_file_path}")

    return Path(config_file_path)


def get_subject_json(subject: str, network: str, data_root: Path) -> Path:
    """
    Returns the path to the subject json file.

    Args:
        subject (str): The subject ID.
        network (str): The network.
        data_root (Path): The path to the data_root directory.

    Returns:
        Path: The path to the subject json file.
    """
    # To sentence case
    network_f = network[0].upper() + network[1:].lower()

    site = subject[0:2].upper()

    return (
        data_root
        / network_f
        / "PHOENIX"
        / "PROTECTED"
        / f"{network_f}{site}"
        / "raw"
        / subject
        / "surveys"
        / f"{subject}.{network_f}.json"
    )


def subject_uses_rpms(config_file: Path, subject_id: str) -> bool:
    query = f"""
    SELECT network_id FROM site WHERE id = '{subject_id[0:2]}';
    """

    network_id = db.fetch_record(config_file=config_file, query=query)

    if network_id is None:
        raise Exception("No network found in the database.")

    if network_id == "PRESCIENT":
        return True
    elif network_id == "ProNET":
        return False
    else:
        raise Exception("Invalid network.")


class NoSubjectConsentDateException(Exception):
    pass


def get_subject_consent_dates(config_file: Path, subject_id: str) -> datetime:
    query = f"""
    SELECT form_data ->> 'chric_consent_date' as consent_date
    FROM forms
    WHERE subject_id = '{subject_id}' AND
        form_name = 'informed_consent_run_sheet' AND
        form_data ? 'chric_consent_date';
    """

    date = db.fetch_record(config_file=config_file, query=query)

    if date is None:
        raise NoSubjectConsentDateException("No consent date found in the database.")
    date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")

    return date


def get_all_subject_forms(config_file: Path, subject_id: str) -> pd.DataFrame:
    query = f"""
    SELECT * FROM forms WHERE subject_id = '{subject_id}';
    """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def str_to_typed(input: str) -> Union[float, int, datetime | str]:
    """
    Converts a string to a typed value.

    Args:
        input (str): The input string to be converted.

    Returns:
        Union[float, int, datetime | str]: The converted value.

    Raises:
        ValueError: If the input string cannot be converted to any of the supported types.
    """
    value = input
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        value = int(value)
        # Handle: MongoDB can only handle up to 8-byte int
        if value > 2147483647 or value < -2147483648:
            value = float(value)
    elif value.replace(".", "", 1).isdigit() or (
        value.startswith("-") and value[1:].replace(".", "", 1).isdigit()
    ):
        value = float(value)
    # Check if matches date format
    elif is_date(value):
        value = datetime.strptime(value, "%Y-%m-%d")
    elif is_time(value):
        value = datetime.strptime(value, "%H:%M")
    elif is_datetime(value):
        value = datetime.strptime(value, "%Y-%m-%d %H:%M")

    return value


def get_days_since_consent(
    config_file: Path, subject_id: str, event_date: datetime
) -> int:
    consent_date = get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )

    return (event_date - consent_date).days + 1


# def check_if_subject_form_data_exists(config_file: Path, subject: str) -> bool:
#     mongodb = db.get_mongo_db(config_file)
#     subject_form_data = mongodb["forms"]

#     subject_form_data_count = subject_form_data.count_documents({"_id": subject})
#     if subject_form_data_count > 0:
#         return True
#     else:
#         return False
