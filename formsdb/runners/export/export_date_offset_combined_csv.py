#!/usr/bin/env python
"""
Export the date shifted combined CSVs.
Master CSVs with all the data in a single file.
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
import multiprocessing
from typing import List, Tuple

import duckdb
import pandas as pd
from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import cli, dpdash, utils

MODULE_NAME = "formsdb.runners.export.export_combined_date_shifted_csv"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

conn = duckdb.connect(database=":memory:")


def get_combined_csvs(config_file: Path) -> List[Path]:
    """
    Get the list of combined CSV files.

    Args:
        config_file (Path): Path to config file

    Returns:
        List[Path]: List of CSV files
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["combined_csvs"])
    csv_files = list(output_dir.glob("*.csv"))

    return csv_files


def get_date_offset_df(config_file: Path) -> pd.DataFrame:
    """
    Construct a DataFrame with date offsets for each subject.

    Uses pattern from config file to look for date offset CSVs from DATA ROOT.

    Args:
        config_file (Path): Path to config file

    Returns:
        pd.DataFrame: DataFrame with date offsets
    """
    general_params = utils.config(path=config_file, section="general")
    data_root = general_params["data_root"]

    date_params = utils.config(path=config_file, section="date_offset")

    date_offset_csv = date_params["date_offset_csv"]
    pattern: str = f"{data_root}/*/{date_offset_csv}"
    logger.debug(f"Looking for date_offset definition files with pattern: {pattern}")

    date_offset_csvs = list(Path(data_root).glob(f"*/{date_offset_csv}"))
    date_offset_df = pd.concat([pd.read_csv(f) for f in date_offset_csvs])

    # reset index
    date_offset_df = date_offset_df.reset_index(drop=True)

    # drop 'stat_hash' and 'upload' columns
    date_offset_df = date_offset_df.drop(columns=["stat_hash", "upload"])

    return date_offset_df


def get_date_cols(config_file: Path) -> List[str]:
    """
    Returns list of variables that are are expected to contain dates.

    Note: Uses Data Dictionary to look for variable names with date validation.

    Args:
        config_file (Path): Path to config file

    Returns:
        List[str]: List of date columns
    """

    data_dictionary_df = data.get_data_dictionary(config_file=config_file)

    duckdb_query = """
    SELECT * FROM data_dictionary_df
    WHERE text_validation LIKE '%date%'
    """

    ddb_rel = duckdb.sql(duckdb_query, connection=conn)
    ddb_df = ddb_rel.df()

    date_cols = ddb_df["variable_name"].tolist()
    return date_cols


def get_combined_date_shifted_csvs_output_root(config_file: Path) -> Path:
    """
    Get the output directory for the combined date shifted CSVs.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["combined_date_shifted_csvs_root"])
    return output_dir


def get_output_file_name(csv_file: Path) -> str:
    """
    Get the output file name for the date shifted CSV file.

    Args:
        csv_file (Path): Path to CSV file

    Returns:
    str: Name of the output file
    """

    dpdash_dict = dpdash.parse_dpdash_name(csv_file.name)
    if dpdash_dict["optional_tags"] is None:
        dpdash_dict["optional_tags"] = []

    dpdash_dict["optional_tags"].append("dateShifted")  # type: ignore

    output_file_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)
    output_file_name = f"{output_file_name}.csv"

    return output_file_name


def process_file(params: Tuple[Path, pd.DataFrame, List[str], Path]) -> None:
    """
    Wrapper function to process a single CSV file.

    Args:
        params (Tuple[Path, pd.DataFrame, List[str], Path]): Tuple of parameters -
            csv_file: Path to CSV file
            date_offset_df: DataFrame with date offsets
            date_cols: List of date columns
            output_root: Path to output root directory
    """
    csv_file, date_offset_df, date_cols, output_root = params

    df = pd.read_csv(csv_file, low_memory=False)
    date_shifted_df = pd.DataFrame()

    shift_subjects = set(date_offset_df["subject"].unique())

    csv_subjects = set(df["subject_id"].unique())
    available_subjects = shift_subjects.intersection(csv_subjects)

    for subject_id in available_subjects:
        subject_df: pd.DataFrame = df[df["subject_id"] == subject_id].copy()
        subject_date_offset_df = date_offset_df[date_offset_df["subject"] == subject_id]
        days_offset = subject_date_offset_df["days"].values[0]
        days_offset_td = pd.to_timedelta(days_offset, unit="D")

        available_date_cols = set(subject_df.columns).intersection(date_cols)
        for date_col in available_date_cols:
            try:
                subject_df[date_col] = (
                    pd.to_datetime(subject_df[date_col]) + days_offset_td
                )
            except ValueError as e:
                if "-3" in str(e):
                    continue
                if "-9" in str(e):
                    continue
                logger.error(
                    f"Error: {e} : {subject_id} : {date_col} : {csv_file.name} "
                )

        date_shifted_df = pd.concat([date_shifted_df, subject_df])

    output_file_name = get_output_file_name(csv_file)
    output_file = output_root / output_file_name
    date_shifted_df.to_csv(output_file, index=False)
    logger.info(f"Saved date shifted file: {output_file}")


def shift_dates(files: List[Path], config_file: Path, output_root: Path) -> None:
    """
    Given a list of CSV files, shift dates based on date_offset CSV.

    Args:
        files (List[Path]): List of CSV files
        config_file (Path): Path to config file
        output_root (Path): Path to output root directory

    Returns:
        None
    """
    date_offset_df = get_date_offset_df(config_file=config_file)
    date_cols = get_date_cols(config_file=config_file)

    logger.debug(f"Found {len(date_offset_df)} date offsets entries")
    logger.debug(f"Found {len(date_cols)} date columns")
    logger.debug(f"Output root: {output_root}")

    with utils.get_progress_bar() as progress:
        files_task = progress.add_task("Processing files", total=len(files))
        params = [(f, date_offset_df, date_cols, output_root) for f in files]
        with multiprocessing.Pool() as pool:
            for _ in pool.imap_unordered(process_file, params):  # type: ignore
                progress.update(files_task, advance=1)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    output_root = get_combined_date_shifted_csvs_output_root(config_file=config_file)
    logger.info(f"Output directory: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)

    cli.clear_directory(output_root, pattern="*.csv")

    csv_files = get_combined_csvs(config_file=config_file)

    logger.info(f"Found {len(csv_files)} combined CSV files")

    shift_dates(files=csv_files, config_file=config_file, output_root=output_root)

    logger.info("Done.")
