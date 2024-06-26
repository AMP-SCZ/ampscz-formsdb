#!/usr/bin/env python
"""
Export the consolidated combined cognitive data.

Consolidates the combined cognitive data and exports it to the outputs directory.

Generates 1 CSV file per network, event type, and visit.
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
from glob import glob
from typing import Dict

import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import cli, dpdash, utils

MODULE_NAME = "formsdb.runners.export.export_consolidated_combined_cognitive"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def construct_output_filename(
    network: str,
    event_name: str,
    df: pd.DataFrame,
) -> str:
    """
    Construct the output filename.

    Template: combined_cognition_{event_type}-{network}-{event_name}-day1to1.csv

    Args:
        network (str): Network name.
        event_name (str): Event name.
        df (pd.DataFrame): Dataframe.

    Returns:
        str: Output filename.
    """
    try:
        event_types = df["event_type"].unique()
    except KeyError:
        event_types = []

    if len(event_types) == 1:
        event_type = event_types[0]

        if pd.isna(event_type):
            event_type = "UNKNOWN"
    elif len(event_types) > 1:
        raise ValueError(f"More than one event type found: {event_types}")
    else:
        event_type = "UNKNOWN"

    custom_name = f"combined_cognition_{event_type}-{network}-{event_name}-day1to1"
    filename = f"{custom_name}.csv"

    return filename


def get_data_dir(config_file: Path) -> Path:
    """
    Get the data directory from the config file.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        Path: Path to the data directory.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["cognitive_combined_outputs_root"])

    return output_dir


def get_output_dir(config_file: Path) -> Path:
    """
    Get the output directory from the config file.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        Path: Path to the output directory.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["cognitive_consolidated_combined_outputs_root"])
    output_dir.mkdir(parents=True, exist_ok=True)

    return output_dir


def consolidate_data(config_file: Path, data_dir: Path, output_dir: Path) -> None:
    """
    Consolidate the combined cognitive data.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        None
    """
    combined_cognitive_files = glob(str(data_dir / "*.csv"))

    network_data: Dict[str, Dict[str, pd.DataFrame]] = {}

    with utils.get_progress_bar() as progress:
        task = progress.add_task(
            "Consolidating data...", total=len(combined_cognitive_files)
        )
        for combined_cognitive_file in combined_cognitive_files:
            progress.advance(task)
            file_path = Path(combined_cognitive_file)
            dp_dash_dict = dpdash.parse_dpdash_name(file_path.name, maxsplit=3)

            network = data.get_network(
                config_file=config_file, site=dp_dash_dict["study"]  # type: ignore
            )
            if network not in network_data:
                logger.debug(f"Adding network {network}...")
                network_data[network] = {}

            if (
                dp_dash_dict["optional_tags"] is not None
                and "SPLLT" in dp_dash_dict["optional_tags"]
            ):
                event_type = "SPLLT"
            elif (
                dp_dash_dict["optional_tags"] is not None
                and "NOSPLLT" in dp_dash_dict["optional_tags"]
            ):
                event_type = "NOSPLLT"
            elif (
                dp_dash_dict["optional_tags"] is not None
                and "nda" in dp_dash_dict["optional_tags"]
            ):
                event_type = "nda"
            else:
                event_type = "UNKNOWN"

            if event_type not in network_data[network]:
                logger.debug(f"Adding event type {event_type}...")
                network_data[network][event_type] = pd.read_csv(combined_cognitive_file)
            else:
                network_data[network][event_type] = pd.concat(
                    [
                        network_data[network][event_type],
                        pd.read_csv(combined_cognitive_file),
                    ]
                )

    visits = constants.upenn_visit_order

    for network, _ in network_data.items():
        for event_type, _ in network_data[network].items():
            for visit in visits:
                df = network_data[network][event_type]
                df = df[df["event_name"].str.contains(visit)]

                if df.empty:
                    continue

                filename = construct_output_filename(
                    network=network, event_name=visit, df=df
                )
                logger.info(f"Writing {filename}...")
                df.to_csv(output_dir / filename, index=False)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    data_dir = get_data_dir(config_file=config_file)
    logger.info(f"Reading data from {data_dir}...")

    output_dir = get_output_dir(config_file=config_file)
    logger.info(f"Writing output to {output_dir}...")

    logger.warning("Clearing existing data...")
    cli.clear_directory(output_dir, pattern="combined_cognition_*-day1to1.csv")

    logger.info("Consolidating data...")
    consolidate_data(
        config_file=config_file,
        data_dir=data_dir,
        output_dir=output_dir,
    )

    logger.info("Done!")
