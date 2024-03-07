#!/usr/bin/env python
"""
Import RPMS Entry Status into PostgreSQL database
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

import pandas as pd
from rich.logging import RichHandler

from formsdb.helpers import utils, db
from formsdb import constants

MODULE_NAME = "formsdb.runners.imports.import_rpms_entry_status"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def import_data(config_file: Path) -> None:
    """
    Imports all RPMS Entry Status data into the PostgreSQL database.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
    config_params = utils.config(config_file, "general")
    data_root = Path(config_params["data_root"])

    entry_status_files = list(
        data_root.glob(
            "Prescient/PHOENIX/PROTECTED/Prescient*/raw/*/surveys/*_entry_status.csv"
        )
    )
    logger.info(f"Found {len(entry_status_files)} RPMS Entry Status files.")

    entry_status_files = sorted(entry_status_files)

    entry_status_df = pd.DataFrame()
    logger.info("Importing RPMS Entry Status data...")
    with utils.get_progress_bar() as progress:
        task = progress.add_task(
            "Importing RPMS Entry Status data...", total=len(entry_status_files)
        )
        for file in entry_status_files:
            progress.update(task, advance=1, description=f"Processing {file.name}")
            df = pd.read_csv(file, low_memory=False)
            entry_status_df = pd.concat([entry_status_df, df])

    entry_status_df = entry_status_df.rename(
        columns={"subjectkey": "subject_id", "InstrumentName": "rpms_form_name"}
    )

    rpms_to_redcap_form_mapping = constants.rmps_to_redcap_form_name
    rpms_to_redcap_visit_mapping = constants.rpms_to_redcap_event

    entry_status_df["redcap_form_name"] = entry_status_df["rpms_form_name"].map(
        rpms_to_redcap_form_mapping
    )
    entry_status_df["redcap_event_name"] = entry_status_df["visit"].map(
        rpms_to_redcap_visit_mapping
    )

    logger.info("Importing RPMS Entry Status data into the database...")
    db.df_to_table(
        config_file=config_file,
        df=entry_status_df,
        table_name="rpms_entry_status",
        if_exists="replace",
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Importing data...")
    import_data(config_file=config_file)

    logger.info("Done.")
