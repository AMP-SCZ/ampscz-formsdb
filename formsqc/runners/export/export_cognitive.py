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
from typing import List

import pandas as pd
from rich.logging import RichHandler

from formsqc.helpers import cli, db, utils

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


required_variables: List[str] = [
    "mpract_valid_code",
    "spcptn90_valid_code",
    "er40_d_valid_code",
    "sfnb2_valid_code",
    "digsym_valid_code",
    "svolt_a_valid_code",
    "sctap_valid_code",
]

dp_dash_required_cols = [
    "reftime",
    "day",
    "timeofday",
    "weekday",
    "subject_id",
]


def construct_output_filename(
    subject_id: str,
    df: pd.DataFrame,
) -> str:
    site_id = subject_id[:2]

    start_day = int(df["day"].min()) if not df.empty else 1
    end_day = int(df["day"].max()) if not df.empty else 1

    filename = (
        f"{site_id}-{subject_id}-form_cognition_summary-day{start_day}to{end_day}.csv"
    )

    return filename


def get_output_dir(config_file: Path) -> Path:
    output_params = utils.config(config_file, "outputs")

    output_dir = Path(output_params["cognitive_summary_outputs_root"])
    return output_dir


def fetch_cognition_data(config_file: Path, subject_id: str) -> pd.DataFrame:
    query = f"""
        SELECT
            *
        FROM cognitive_data
        WHERE
            subject_id = '{subject_id}'
        ORDER BY
            day ASC;
    """

    df = db.execute_sql(config_file, query)

    return df


def make_df_dpdash_ready(df: pd.DataFrame) -> pd.DataFrame:
    global dp_dash_required_cols

    for col in dp_dash_required_cols:
        if col not in df.columns:
            df[col] = ""

    cols: List[str] = df.columns.tolist()
    cols = [col for col in cols if col not in dp_dash_required_cols]

    df = df[dp_dash_required_cols + cols]

    # Check if [day] is filled in, else fill it in with 1 to n
    # replace empty strings with NA
    df.replace("", pd.NA, inplace=True)
    if df["day"].isnull().values.any():  # type: ignore
        df["day"] = df["day"].ffill()
        df["day"] = df["day"].bfill()
        df["day"] = df["day"].fillna(1)

    return df


def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    global dp_dash_required_cols
    df = df.reset_index(drop=True)

    try:
        subject_id = df["subject_id"].iloc[0]
    except IndexError:
        subject_id = pd.NA

    cols_to_drop = [col for col in dp_dash_required_cols if col in df.columns]
    df.drop(columns=cols_to_drop, inplace=True)

    df.set_index("event_name", inplace=True, drop=True)

    df = df.unstack().to_frame().T  # type: ignore
    df.columns = df.columns.map("_".join)

    df["subject_id"] = subject_id

    return df


def generate_csv(
    config_file: Path, subject_id: str, output_dir: Path, flatten: bool = True
) -> None:
    df = fetch_cognition_data(config_file=config_file, subject_id=subject_id)

    if df.shape[0] == 0:
        logger.warning(f"No cognition data found for {subject_id}.")

    output_filename = construct_output_filename(subject_id=subject_id, df=df)
    output_path = output_dir / output_filename

    if flatten:
        df = flatten_df(df=df)

    df = make_df_dpdash_ready(df=df)

    df.to_csv(output_path, index=False)


def export_data(config_file: Path, output_dir: Path) -> None:
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

            generate_csv(
                config_file=config_file, subject_id=subject_id, output_dir=output_dir
            )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    output_dir = get_output_dir(config_file=config_file)
    logger.info(f"Writing output to {output_dir}...")
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.warning("Clearing existing data...")
    cli.clear_directory(output_dir)

    logger.info("Exporting data...")
    export_data(config_file=config_file, output_dir=output_dir)

    logger.info("Done.")
