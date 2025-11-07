#!/usr/bin/env python
"""
Import Outcomes calculated by `https://github.com/AMP-SCZ/outcome_calculations`
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
from typing import List, Tuple
from glob import glob
import multiprocessing

import pandas as pd
from rich.logging import RichHandler

from formsdb.helpers import utils, db
from formsdb import constants

MODULE_NAME = "formsdb.runners.imports.import_calculated_outcomes"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def process_form(
    form_path: Path,  # CSV file path
) -> Tuple[bool, Path, pd.DataFrame]:
    """
    Process a single form CSV file and return a DataFrame.
    
    Args:
        form_path (Path): The path to the form CSV file.
    
    Returns:
        Tuple[bool, Path, pd.DataFrame]: A tuple containing a boolean indicating if the
        form was imported, the form path, and the processed DataFrame.
    """
    form_parts = form_path.parts

    subject_id = form_parts[9]
    form_name = form_path.stem
    source_m_date = utils.get_file_mtime(form_path)

    form_df = pd.read_csv(form_path)

    required_cols = ["variable", "redcap_event_name", "value", "data_type"]
    form_df_filtered = form_df[required_cols]

    # Drop missing codes (-300, -900)
    form_df_filtered = form_df_filtered[
        ~form_df_filtered["value"].isin([-300, -900, "-300", "-900", "03/03/1903", "09/09/1909"])
    ]

    form_df_filtered["source_m_date"] = source_m_date
    form_df_filtered["subject_id"] = subject_id
    form_df_filtered["form_name"] = form_name

    form_df_filtered = form_df_filtered[
        [
            "subject_id",
            "form_name",
            "redcap_event_name",
            "variable",
            "value",
            "data_type",
            "source_m_date",
        ]
    ]

    return form_df_filtered.empty, form_path, form_df_filtered


def process_form_wrapper(
    form_path: Path,  # CSV file path
) -> Tuple[bool, Path, pd.DataFrame]:
    """
    Wrapper function to process a form CSV file and return a DataFrame.
    """
    try:
        return process_form(form_path)
    except Exception as e:
        logger.error(f"Error processing {form_path}: {e}")
        return False, form_path, pd.DataFrame()


def get_forms_by_network(
    network: str,
    data_root: Path,
) -> pd.DataFrame:
    """
    Import forms data by reading JSON files from the data root.

    Args:
        config_file (Path): The path to the config file.
        network (str): The network name.
        data_root (Path): The path to the data root.
        data_dictionary (Path): The path to the data dictionary.

    Returns:
        None
    """
    search_pattern = f"{data_root}/{network}/PHOENIX/GENERAL/*/processed/*/surveys/*.csv"
    logger.debug(f"Searching for forms with pattern: {search_pattern}")
    forms_glob = glob(search_pattern)
    forms_glob = sorted(forms_glob, reverse=False)
    logger.info(f"Found {len(forms_glob)} forms for {network}")

    skipped_forms: List[Path] = []
    processed_forms: List[Path] = []

    num_processes = 8
    logger.info(f"Using {num_processes} processes.")

    params = [Path(form) for form in forms_glob]
    master_df = pd.DataFrame()

    print_freq_percent = 10

    with utils.get_progress_bar() as progress:
        with multiprocessing.Pool(num_processes) as pool:
            task = progress.add_task(
                f"Processing {network} JSONs...", total=len(forms_glob)
            )
            processed_count = 0
            total_forms = len(forms_glob)
            for result in pool.imap_unordered(process_form_wrapper, params):
                imported, form_path, form_df = result
                if imported:
                    processed_forms.append(form_path)
                else:
                    skipped_forms.append(form_path)
                master_df = pd.concat([master_df, form_df], ignore_index=True)
                processed_count += 1
                progress.update(task, advance=1)
                if processed_count % max(1, total_forms * print_freq_percent // 100) == 0:
                    progress_msg = (
                        f"Progress: {processed_count}/"
                        f"{total_forms} ("
                        f"{processed_count * 100 // total_forms}"
                        f"%) | "
                        f"Processed: {len(processed_forms)} ("
                        f"{len(processed_forms) * 100 // processed_count if processed_count else 0}"
                        f"%) | "
                        f"Skipped: {len(skipped_forms)} ("
                        f"{len(skipped_forms) * 100 // processed_count if processed_count else 0}"
                        f"%)"
                    )
                    logger.info(progress_msg)

    logger.info(f"Processed {len(processed_forms)} forms")
    if len(skipped_forms) > 0:
        logger.warning(f"Failed to import {len(skipped_forms)} forms")

    return master_df

if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    data_root = Path(config_params["data_root"])
    networks = constants.networks

    logger.info(f"Importing calculated outcomes from {data_root}")

    master_df = pd.DataFrame()
    for network in networks:
        logger.info(f"Processing network: {network}")
        network_df = get_forms_by_network(network, data_root)
        master_df = pd.concat([master_df, network_df], ignore_index=True)

    if not master_df.empty:
        logger.debug("Importing calculated outcomes to database...")
        logger.debug(f"Master DataFrame shape: {master_df.shape}")
        db.df_to_table(
            config_file=config_file,
            df=master_df,
            table_name="calculated_outcomes",
            schema="forms_derived",
            if_exists="replace",
        )
        logger.info(f"Imported {len(master_df)} calculated outcomes.")

    logger.info("Done!")
