#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "ampscz-formsqc":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging

import pandas as pd
from rich.logging import RichHandler

from formsqc.helpers import cli, db, dpdash, utils
from formsqc import data

MODULE_NAME = "formsqc_cognitive_exporter"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def fetch_avaiability_data(config_file: Path, subject_id: str) -> pd.DataFrame:
    query = f"""
        SELECT * FROM cognitive_data_availability
        WHERE subject_id='{subject_id}'
        """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def fetch_summary_data(config_file: Path, subject_id: str) -> pd.DataFrame:
    query = f"""
        SELECT * FROM cognitive_summary
        WHERE subject_id='{subject_id}'
        ORDER BY day;
        """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def generate_upenn_data_summary_filename(
    subject_id: str,
    df: pd.DataFrame,
) -> str:
    max_days = df["day"].max()

    filename = dpdash.get_dpdash_name(
        study=subject_id[:2],
        subject=subject_id,
        data_type="forms",
        category="cognitive",
        optional_tag=["data", "summary"],
        time_range=f"day1to{max_days}",
    )

    return filename


def generate_upenn_data_availability_filename(
    subject_id: str,
    df: pd.DataFrame,
) -> str:
    max_days = df["day"].max()

    filename = dpdash.get_dpdash_name(
        study=subject_id[:2],
        subject=subject_id,
        data_type="forms",
        category="cognitive",
        optional_tag=["data", "availability"],
        time_range=f"day1to{max_days}",
    )

    return filename


def get_availability_output_dir(config_file: Path) -> Path:
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["cognitive_availability_outputs_root"])
    return output_dir


def get_summary_output_dir(config_file: Path) -> Path:
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["cognitive_summary_outputs_root"])
    return output_dir


def generate_csvs(
    config_file: Path,
    subject_id: str,
    availability_output_dir: Path,
    summary_output_dir: Path,
) -> None:
    availability_df = fetch_avaiability_data(
        config_file=config_file, subject_id=subject_id
    )
    summary_df = fetch_summary_data(config_file=config_file, subject_id=subject_id)

    if not availability_df.empty:
        df = data.make_df_dpdash_ready(subject_id=subject_id, df=availability_df)
        filename = generate_upenn_data_availability_filename(
            subject_id=subject_id,
            df=df,
        )

        filepath = availability_output_dir / f"{filename}.csv"
        df.to_csv(filepath, index=False)
    else:
        logger.warning(f"No availability data for {subject_id}.")

    if not summary_df.empty:
        filename = generate_upenn_data_summary_filename(
            subject_id=subject_id,
            df=summary_df,
        )

        filepath = summary_output_dir / f"{filename}.csv"
        summary_df.to_csv(filepath, index=False)
    else:
        logger.warning(f"No summary data for {subject_id}.")


def export_data(
    config_file: Path, availability_output_dir: Path, summary_output_dir: Path
) -> None:
    subject_query = """
        SELECT DISTINCT subject_id FROM upenn_forms ORDER BY subject_id ASC;
    """

    subject_id_df = db.execute_sql(config_file, subject_query)
    subject_ids = subject_id_df["subject_id"].tolist()

    subjects_count = len(subject_ids)
    logger.info(f"Exporting data for {subjects_count} subjects...")

    with utils.get_progress_bar() as progress:
        task = progress.add_task("Processing...", total=subjects_count)

        for subject_id in subject_ids:
            progress.update(task, advance=1, description=f"Processing {subject_id}...")

            generate_csvs(
                config_file=config_file,
                subject_id=subject_id,
                availability_output_dir=availability_output_dir,
                summary_output_dir=summary_output_dir,
            )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    availability_output_dir = get_availability_output_dir(config_file=config_file)
    summary_output_dir = get_summary_output_dir(config_file=config_file)

    logger.info(f"Writing availability output to {availability_output_dir}...")
    logger.info(f"Writing summary output to {summary_output_dir}...")

    availability_output_dir.mkdir(parents=True, exist_ok=True)
    summary_output_dir.mkdir(parents=True, exist_ok=True)

    logger.warning("Clearing existing data...")
    cli.clear_directory(availability_output_dir)
    cli.clear_directory(summary_output_dir)

    logger.info("Exporting data...")
    export_data(
        config_file=config_file,
        availability_output_dir=availability_output_dir,
        summary_output_dir=summary_output_dir,
    )

    logger.info("Done.")
