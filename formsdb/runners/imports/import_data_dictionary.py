#!/usr/bin/env python
"""
Import Data Dictionary into Postgres
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

from formsdb.helpers import db, utils

MODULE_NAME = "formsdb.runners.imports.import_data_dictionary"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    data_params = utils.config(path=config_file, section="data")
    updated_data_dictionary_path = Path(data_params["updated_data_dictionary"])

    logger.info(f"Reading updated data dictionary from {updated_data_dictionary_path}")

    data_dictionary = pd.read_csv(updated_data_dictionary_path)
    db.df_to_table(
        config_file=config_file,
        df=data_dictionary,
        table_name="data_dictionary",
        if_exists="replace",
    )

    logger.info("Data dictionary imported successfully")