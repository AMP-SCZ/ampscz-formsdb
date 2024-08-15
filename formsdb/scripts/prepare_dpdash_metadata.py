#!/usr/bin/env python
"""
Prepares metadata for import into DP Dash
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

import argparse
import logging
import glob
from typing import List
import shutil

from rich.logging import RichHandler

from formsdb.helpers import utils, cli
from formsdb import constants

MODULE_NAME = "formsdb.scripts.wipe_dpdash"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def rename_files(files: List[Path], output_directory: Path) -> None:
    """
    Remove network name from metadata files.

    Args:
        files (List[Path]): List of metadata files
        output_directory (Path): Output directory

    Returns:
        None
    """
    networks: List[str] = constants.networks

    for file in files:
        file_name = file.name
        for network in networks:
            if network in file_name:
                new_file_name = file_name.replace(network, "")
                new_file_path = output_directory / new_file_name

                logger.debug(f"Copied {file} to {new_file_path}")
                shutil.copy(file, new_file_path)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    parser = argparse.ArgumentParser(description="Wipe DPDash data")
    parser.add_argument(
        "-m",
        "--metadata-patern",
        type=str,
        help="Glob pattern for metadata files",
        required=True,
        # default="/data/predict1/data_from_nda/Pr*/PHOENIX/PROTECTED/P*/P*_metadata.csv"
    )
    parser.add_argument(
        "-o",
        "--output-path",
        type=str,
        help="Path to the directory to save the output files",
        required=True,
        # default="/data/predict1/home/dm1447/data"
    )

    args = parser.parse_args()

    metadata_pattern = Path(args.metadata_patern)
    output_directory_path = Path(args.output_path)

    logger.debug(f"Metadata pattern: {metadata_pattern}")
    logger.debug(f"Output directory: {output_directory_path}")

    cli.clear_directory(directory=output_directory_path, logger=logger, pattern="*_metadata.csv")

    metadata_files = list(glob.glob(str(metadata_pattern)))
    logger.info(f"Found {len(metadata_files)} metadata files")

    metadata_files = [Path(file) for file in metadata_files]

    rename_files(metadata_files, output_directory_path)
