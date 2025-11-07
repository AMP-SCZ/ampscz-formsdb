#!/usr/bin/env python
"""
Export the filters for DP Dash.
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
from typing import Tuple

import pandas as pd
from rich.logging import RichHandler

from formsdb import data
from formsdb.helpers import cli, db, dpdash, utils

MODULE_NAME = "formsdb.runners.export.export_filters"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def fetch_recruitment_data(config_file: Path, subject_id: str) -> pd.DataFrame:
    """
    Fetch the recruitment data for a subject.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.

    Returns:
        pd.DataFrame: DataFrame containing the recruitment data for the subject.
    """
    query = f"""
        SELECT forms_derived.filters.subject AS subject_id,
            forms_derived.filters.gender,
            forms_derived.filters.cohort,
            forms_derived.filters.chrcrit_included,
            forms_derived.recruitment_status.recruitment_status_v2 AS recruitment_status
        FROM forms_derived.filters
        LEFT JOIN forms_derived.recruitment_status ON filters.subject = forms_derived.recruitment_status.subject_id
        WHERE subject='{subject_id}'
        """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def generate_filter_filename(subject_id: str) -> str:
    """
    Generate DP Dash compliant the filename for the filters data.
    Args:
        subject_id (str): Subject ID.

    Returns:
        str: Filename for the UPenn data summary.
    """

    filename = dpdash.get_dpdash_name(
        study=subject_id[:2],
        subject=subject_id,
        data_type="form",
        category="filters",
        # optional_tag=["status"],
        optional_tag=[],
        time_range="day1to1",
    )

    return f"{filename}.csv"


def get_filters_output_dir(config_file: Path) -> Path:
    """
    Get the output directory for the filter data.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["filters_root"])
    return output_dir


def generate_csv(
    config_file: Path,
    subject_id: str,
    output_dir: Path,
) -> str:
    """
    Generate the CSV file for the filter data.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        output_dir (Path): Path to the output directory.
        
    Returns:
        str: Subject ID for tracking completion.
    """
    df = fetch_recruitment_data(config_file=config_file, subject_id=subject_id)

    if df.empty:
        logger.warning(f"No recruitment data found for subject {subject_id}.")
        return subject_id

    filename = generate_filter_filename(subject_id=subject_id)
    output_path = output_dir / filename

    df = data.make_df_dpdash_ready(df=df, subject_id=subject_id)

    # recruitment_status -> Map to only `recruited`, `not_recruited`
    df["recruitment_status"] = df["recruitment_status"].apply(
        lambda x: "recruited" if x == "recruited" else "not recruited"
    )

    df.to_csv(output_path, index=False)
    return subject_id


def process_subject_wrapper(args: Tuple[Path, str, Path]) -> str:
    """
    Wrapper for multiprocessing to unpack arguments.
    
    Args:
        args: Tuple containing (config_file, subject_id, output_dir)
        
    Returns:
        str: Subject ID for tracking completion.
    """
    config_file, subject_id, output_dir = args
    return generate_csv(
        config_file=config_file,
        subject_id=subject_id,
        output_dir=output_dir,
    )


def export_data(config_file: Path, output_root: Path) -> None:
    """
    Export the recruitment status data to CSVs using multiprocessing.

    Args:
        config_file (Path): Path to the config file.
        output_root (Path): Path to the output root directory.
    """
    subject_ids = data.get_all_subjects(config_file=config_file)
    
    num_processes = 8
    logger.info(f"Using {num_processes} processes...")
    params = [
        (config_file, subject_id, output_root)
        for subject_id in subject_ids
    ]
    log_frequency = max(1, len(params) // 10)

    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        complete_count = 0
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Processing subjects...", total=len(params))
            for _ in pool.imap_unordered(process_subject_wrapper, params):
                complete_count += 1
                if complete_count % log_frequency == 0 or complete_count == len(params):
                    logger.info(f"Processed {complete_count}/{len(params)} subjects...")
                progress.update(task, advance=1)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    output_root = get_filters_output_dir(config_file=config_file)

    logger.info(f"Exporting recruitment status data to {output_root}...")

    output_root.mkdir(parents=True, exist_ok=True)

    logger.warning("Clearing existing files in the output directory...")
    cli.clear_directory(output_root, pattern="*form_filters*.csv")

    logger.info("Exporting data...")
    export_data(config_file=config_file, output_root=output_root)

    logger.info("Done.")
