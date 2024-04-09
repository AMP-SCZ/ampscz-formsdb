#!/usr/bin/env python
"""
Export the blood metrics of subjects to CSVs.
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

MODULE_NAME = "formsdb.runners.export.export_blood_metrics"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def fetch_blood_metrics(
    config_file: Path, subject_id: str, event_name: str
) -> pd.DataFrame:
    """
    Fetch the blood metrics for a subject, for a specific event.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        event_name (str): Event name.

    Returns:
        pd.DataFrame: DataFrame containing the blood metrics for the subject.
    """
    query = f"""
        SELECT *
        FROM blood_metrics
        WHERE subject_id='{subject_id}'
            AND event_name LIKE '{event_name}_%%'
        """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def generate_blood_metrics_filename(subject_id: str, event_name: str) -> str:
    """
    Generate DP Dash compliant the filename for the blood metrics data.
    Args:
        subject_id (str): Subject ID.
        event_name (str): Event name.

    Returns:
        str: Filename for the blood metrics data.
    """

    filename = dpdash.get_dpdash_name(
        study=subject_id[:2],
        subject=subject_id,
        data_type="form",
        category="bloodMetrics",
        optional_tag=[event_name],
        time_range="day1to1",
    )

    return f"{filename}.csv"


def get_blood_metrics_output_dir(config_file: Path) -> Path:
    """
    Get the output directory for the blood metrics data.
    """
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["blood_metrics_root"])
    return output_dir


def generate_csv(
    config_file: Path,
    subject_id: str,
    event_name: str,
    output_dir: Path,
) -> None:
    """
    Generate the CSV file for the blood metrics data.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        output_dir (Path): Path to the output directory.
    """
    df = fetch_blood_metrics(
        config_file=config_file, subject_id=subject_id, event_name=event_name
    )

    if df.empty:
        # logger.warning(
        #     f"No blood metrics data found for subject {subject_id}: {event_name}"
        # )
        return

    filename = generate_blood_metrics_filename(
        subject_id=subject_id, event_name=event_name
    )
    output_path = output_dir / filename

    df = data.make_df_dpdash_ready(df=df, subject_id=subject_id)

    df.to_csv(output_path, index=False)


def export_data(config_file: Path, output_root: Path) -> None:
    """
    Export the blood metrics data to CSVs.

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
                event_name="baseline",
            )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    output_root = get_blood_metrics_output_dir(config_file=config_file)

    logger.info(f"Exporting blood metrics to {output_root}...")

    output_root.mkdir(parents=True, exist_ok=True)

    logger.warning("Clearing existing files in the output directory...")
    cli.clear_directory(output_root, pattern="*form_bloodMetrics*.csv")

    logger.info("Exporting data...")
    export_data(config_file=config_file, output_root=output_root)

    logger.info("Done.")
