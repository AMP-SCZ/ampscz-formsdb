#!/usr/bin/env python
"""
Import ClientStatus CSVs to PostgreSQL.

ClientStatus CSVs unique to RPMS, and contain dates associated with different
timepoints / milestones.
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
from typing import List

from rich.logging import RichHandler
import pandas as pd

from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.imports.import_client_status"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_all_client_status_csvs(data_root: Path) -> List[Path]:
    """
    Get all ClientStatus CSVs from data_root.

    Args:
        data_root (Path): Root directory containing ClientStatus CSVs.

    Returns:
        List[Path]: List of all ClientStatus CSVs.
    """
    search_path = "*/PHOENIX/PROTECTED/*/raw/*/surveys/*_ClientStatus_AllDates.csv"
    logger.info(f"Searching for ClientStatus CSVs in {data_root}/{search_path}")
    client_status_csvs = list(
        data_root.glob(search_path)
    )
    logger.info(f"Found {len(client_status_csvs)} ClientStatus CSVs")
    return client_status_csvs


def import_client_status_files(config_file: Path) -> None:
    """
    Import ClientStatus CSVs to DB.

    Args:
        config_file (Path): Path to config file.
    """
    config_params = utils.config(config_file, "general")
    data_root = Path(config_params["data_root"])

    with utils.get_progress_bar() as progress:
        task = progress.add_task("Looking for ClientStatus CSVs...", total=None)
        client_status_csvs = get_all_client_status_csvs(data_root)
        progress.remove_task(task)

    master_df = pd.DataFrame()

    with utils.get_progress_bar() as progress:
        task = progress.add_task(
            "Importing ClientStatus CSVs...", total=len(client_status_csvs)
        )
        for file in client_status_csvs:
            progress.update(task, advance=1)
            df = pd.read_csv(file)
            master_df = pd.concat([master_df, df])

    # Rename subjectkey column to subject_id
    master_df.rename(columns={"subjectkey": "subject_id"}, inplace=True)

    logger.info(f"Writing {len(master_df)} records to DB")
    db.df_to_table(
        df=master_df,
        table_name="rpms_client_status",
        schema="forms",
        if_exists="replace",
        config_file=config_file,
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Importing ClientStatus CSVs to DB")
    import_client_status_files(config_file=config_file)

    logger.info("Done!")
