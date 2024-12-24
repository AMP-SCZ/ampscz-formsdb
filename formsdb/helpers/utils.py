"""
Helper functions for the pipeline
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from formsdb.helpers import cli
from formsdb.helpers.config import config

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
    log_file_r = log_params[module_name]

    if log_file_r.startswith("/"):
        log_file = Path(log_file_r)
    else:
        general_params = config(config_file, "general")
        repo_root = Path(general_params["repo_root"])

        log_file = repo_root / log_file_r

    if log_file.exists() and log_file.stat().st_size > 10000000:  # 10MB
        archive_file = (
            log_file.parent
            / "archive"
            / f"{log_file.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
        )
        logger.info(f"Rotating log file to {archive_file}")

        archive_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.rename(archive_file)

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  - %(process)d - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]"
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
    config_file_path = repo_root / "config.ini"

    # Check if config_file_path exists
    if not config_file_path.is_file():
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


def str_to_typed(value: str) -> Union[float, int, datetime | str]:
    """
    Converts a string to a typed value.

    Args:
        input (str): The input string to be converted.

    Returns:
        Union[float, int, datetime | str]: The converted value.

    Raises:
        ValueError: If the input string cannot be converted to any of the supported types.
    """
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        value = int(value)  # type: ignore
        # Handle: MongoDB can only handle up to 8-byte int
        if value > 2147483647 or value < -2147483648:  # type: ignore
            value = float(value)  # type: ignore
    elif value.replace(".", "", 1).isdigit() or (
        value.startswith("-") and value[1:].replace(".", "", 1).isdigit()
    ):
        value = float(value)  # type: ignore
    # Check if matches date format
    elif is_date(value):
        value = datetime.strptime(value, "%Y-%m-%d")  # type: ignore
    elif is_time(value):
        value = datetime.strptime(value, "%H:%M")  # type: ignore
    elif is_datetime(value):
        value = datetime.strptime(value, "%Y-%m-%d %H:%M")  # type: ignore

    return value


def explode_col(df: pd.DataFrame, col: str = "form_data") -> pd.DataFrame:
    """
    Explodes the `col` column of the DataFrame.

    `col` column contains a JSON object.

    Args:
        df (pd.DataFrame): DataFrame containing the `col`.
        col (str, optional): The name of the column to explode. Defaults to "form_data".

    Returns:
        pd.DataFrame: DataFrame with the `col` column exploded.
    """
    df.reset_index(drop=True, inplace=True)
    df = pd.concat(
        [df.drop(col, axis=1), pd.json_normalize(df[col])],  # type: ignore
        axis=1,
    )

    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the DataFrame.

    - Removes '\n' from columns
    - replace all occurrences of '.0' in values with ''
    - replace None with ''

    Args:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    # replace None with ''
    df = df.astype(str).replace("NaT", "")

    # replace all occurrences of '.00' in values with ''
    df = df.astype(str).replace(r"\.0+$", "", regex=True)

    # replace None with ''
    df = df.astype(str).replace("None", "")

    # replace all '/n', '/r', '/t' with '' to prevent line breaks
    df = df.replace(r"\n", "", regex=True)
    df = df.replace(r"\r", "", regex=True)
    df = df.replace(r"\t", "", regex=True)

    # replace nan with ''
    df = df.astype(str).replace("nan", "")

    # replace '<NA>' with ''
    df = df.replace("<NA>", "")

    return df
