#!/usr/bin/env python
"""
Export the visit status of subjects to CSVs.
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

from formsdb import data
from formsdb.helpers import cli, db, dpdash, utils

MODULE_NAME = "formsdb.runners.export.export_visit_status"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def fetch_visit_data(config_file: Path, subject_id: str) -> pd.DataFrame:
    """
    Fetch the visit data for a subject.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.

    Returns:
        pd.DataFrame: DataFrame containing the visit data for the subject.
    """
    query = f"""
        SELECT * FROM forms_derived.subject_visit_status
        LEFT JOIN forms_derived.subject_visit_completed USING(subject_id)
        WHERE subject_id='{subject_id}'
        """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def fetch_visit_data_w_converted_withdrawn(
    config_file: Path, subject_id: str
) -> pd.DataFrame:
    """
    Fetch the visit data for a subject.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.

    Returns:
        pd.DataFrame: DataFrame containing the visit data for the subject.
    """
    query = f"""
        SELECT
            subject_id,
            CASE
                WHEN converted THEN 'converted'
                WHEN removed THEN 'withdrawn'
                ELSE timepoint
            END AS visit_status
        FROM forms_derived.subject_visit_status
        LEFT JOIN forms_derived.conversion_status USING(subject_id)
        LEFT JOIN forms_derived.subject_removed USING(subject_id)
        WHERE subject_id='{subject_id}'
        """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def generate_visit_status_filename(subject_id: str, extended: bool = False) -> str:
    """
    Generate DP Dash compliant the filename for the Visit Status data.
    Args:
        subject_id (str): Subject ID.
        extended (bool): Whether to include the extended tag in the filename.

    Returns:
        str: Filename for the Visit Status data.
    """

    optional_tag = ["status"]
    if extended:
        optional_tag.append("extended")

    filename = dpdash.get_dpdash_name(
        study=subject_id[:2],
        subject=subject_id,
        data_type="form",
        category="visit",
        optional_tag=optional_tag,
        time_range="day1to1",
    )

    return f"{filename}.csv"


def get_visit_status_output_dir(config_file: Path) -> Path:
    """
    Get the output directory for the visit status data.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["visit_status_root"])
    return output_dir


def generate_csv(
    config_file: Path,
    subject_id: str,
    output_dir: Path,
    extended: bool = False,
) -> None:
    """
    Generate the CSV file for the visit status data.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        output_dir (Path): Path to the output directory.
        extended (bool): If true generated files with 'withdrawn' and 'converted' status.
    """
    if extended:
        df = fetch_visit_data_w_converted_withdrawn(
            config_file=config_file, subject_id=subject_id
        )
    else:
        df = fetch_visit_data(config_file=config_file, subject_id=subject_id)

    if df.empty:
        logger.warning(f"No visit data found for subject {subject_id}.")
        return

    filename = generate_visit_status_filename(subject_id=subject_id, extended=extended)
    output_path = output_dir / filename

    df = data.make_df_dpdash_ready(df=df, subject_id=subject_id)

    df.to_csv(output_path, index=False)


def export_data(config_file: Path, output_root: Path) -> None:
    """
    Export the visit status data to CSVs.

    Args:
        config_file (Path): Path to the config file.
        output_root (Path): Path to the output root directory.
    """
    subject_ids = data.get_all_subjects(config_file=config_file)

    with utils.get_progress_bar() as progress:
        task = progress.add_task("Processing...", total=len(subject_ids))

        for subject_id in subject_ids:
            progress.update(task, advance=1, description=f"Processing {subject_id}...")
            generate_csv(
                config_file=config_file,
                subject_id=subject_id,
                output_dir=output_root,
            )

    logger.info("Exporting Visit Status data with extended status...")
    with utils.get_progress_bar() as progress:
        task = progress.add_task("Processing...", total=len(subject_ids))

        for subject_id in subject_ids:
            progress.update(task, advance=1, description=f"Processing {subject_id}...")
            generate_csv(
                config_file=config_file,
                subject_id=subject_id,
                output_dir=output_root,
                extended=True,
            )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    output_root = get_visit_status_output_dir(config_file=config_file)

    logger.info(f"Exporting visit status data to {output_root}...")

    output_root.mkdir(parents=True, exist_ok=True)

    logger.warning("Clearing existing files in the output directory...")
    cli.clear_directory(output_root, pattern="*form_visit_status*.csv")

    logger.info("Exporting data...")
    export_data(config_file=config_file, output_root=output_root)

    logger.info("Done.")
