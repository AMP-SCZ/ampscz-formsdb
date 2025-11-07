#!/usr/bin/env python
"""
Consolidate metrics from dpdash sources into a single dataframe.
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
from typing import Dict, List

import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import cli, db, dpdash, utils

MODULE_NAME = "formsdb.runners.export.dpdash_charts"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_dpdash_db_sources_map(config_file: Path) -> Dict[str, List[str]]:
    """
    Get the mapping of sources to variables from the dpdash config file.

    Args:
        config_file (Path): Path to the dpdash config file.

    Returns:
        Dict[str, List[str]]: Mapping of sources to variables.
    """

    dpdash_sources = utils.config(path=config_file, section="dpdash-csv-tables")

    dpdash_sources_variables_map = {}

    for source_tables, source_columns_str in dpdash_sources.items():
        source_columns = [col.strip() for col in source_columns_str.split(",")]
        dpdash_sources_variables_map[source_tables] = source_columns

    return dpdash_sources_variables_map


def construct_master_df(
    dpdash_db_sources_map: Dict[str, List[str]],
    config_file: Path,
) -> pd.DataFrame:
    """
    Construct a master dataframe by combining data from all dpdash sources.

    Args:
        dpdash_db_sources_map (Dict[str, List[str]]): Mapping of sources to variables.

    Returns:
        pd.DataFrame: Master dataframe.
    """

    master_df = pd.DataFrame(dtype=str)

    with utils.get_progress_bar() as progress:
        sources_task = progress.add_task(
            "Processing sources", total=len(dpdash_db_sources_map)
        )
        for table, columns in dpdash_db_sources_map.items():
            progress.update(
                sources_task, advance=1, description=f"Processing source: {table}"
            )

            query = f"""
                SELECT subject_id,
                          {', '.join(columns)}
                FROM {table}
            """

            temp_df = db.execute_sql(
                config_file=config_file,
                query=query,
            )

            if master_df.empty:
                master_df = temp_df
            else:
                master_df = pd.merge(
                    master_df,
                    temp_df,
                    on="subject_id",
                    how="outer",
                )

    return master_df


def make_df_dpdash_ready(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make the dataframe ready for dpdash by adding DPDash required columns
    and adding the subject_id column.

    Args:
        df (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Output dataframe.
    """

    required_columns = ["reftime", "day", "timeofday", "weekday"]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    df["day"] = 1

    return df


def append_nr_to_non_recruited_subjects(master_df: pd.DataFrame) -> pd.DataFrame:
    """
    Differentiate between recruited and non-recruited subjects.

    Append _n_r to all the other statuses for non-recruited subjects.

    Args:
        master_df (pd.DataFrame): Master dataframe.

    Returns:
        pd.DataFrame: Master dataframe with _n_r appended to non-recruited subjects.
    """
    master_df = master_df.fillna("nan")

    for idx, row in master_df.iterrows():
        recruitment_status = row["recruitment_status"]

        if recruitment_status != "recruited":
            # append n_r to all the other statuses
            for col in master_df.columns:
                if (
                    col not in constants.dp_dash_required_cols
                    and col not in constants.skip_adding_nr
                ):
                    master_df.loc[idx, col] = f"{master_df.loc[idx, col]}_n_r"  # type: ignore

    return master_df


def generate_dpdash_imported_csvs(
    master_df: pd.DataFrame, data_root: Path, config_file: Path
) -> None:
    """
    Generate the DPDash imported csvs.

    Args:
        master_df (pd.DataFrame): Master dataframe.
    """
    with utils.get_progress_bar() as progress:
        export_task = progress.add_task("Exporting data", total=len(master_df))
        for _, row in master_df.iterrows():
            subject_id = row["subject_id"]
            progress.update(
                export_task,
                advance=1,
                description=f"Exporting data for {subject_id}",
            )
            subject_network = data.get_subject_network(
                subject_id=subject_id, config_file=config_file
            )
            subject_site = subject_id[:2]
            output_root: Path = (
                data_root
                / subject_network.capitalize()
                / "PHOENIX"
                / "PROTECTED"
                / f"{subject_network.capitalize()}{subject_site}"
                / "processed"
                / subject_id
                / "surveys"
            )

            if not output_root.exists():
                try:
                    output_root.mkdir(parents=True)
                    cli.set_permissions(
                        path=output_root, permissions="775", silence_logs=True
                    )
                except PermissionError:
                    logger.error(f"Could not create directory: {output_root}")
                    continue

            dpdash_name = dpdash.get_dpdash_name(
                subject=subject_id,
                study=subject_site,
                data_type="form",
                category="dpdash",
                optional_tag=["charts"],
                time_range="day1to1",
            )

            cli.clear_directory(
                directory=output_root, pattern="*dpdash_charts*.csv", silence_logs=True
            )
            export_path = Path(output_root) / f"{dpdash_name}.csv"

            df_part = row.to_frame().T
            df_part.to_csv(export_path, index=False)
            cli.set_permissions(path=export_path, permissions="664", silence_logs=True)


def main(config_file: Path) -> None:
    """
    Main function to merge metrics from dpdash sources.

    Args:
        config_file (Path): Path to the dpdash config file.
    """
    logger.info(f"Using config file: {config_file}")

    general_params = utils.config(path=config_file, section="general")
    output_params = utils.config(path=config_file, section="outputs")
    data_root = Path(general_params["data_root"])
    output_root = Path(output_params["dpdash_chart_statuses_root"])

    logger.info(f"Data root: {data_root}")

    dpdash_sources_variables_map = get_dpdash_db_sources_map(config_file)
    logger.debug(f"dpdash_sources_variables_map: {dpdash_sources_variables_map}")

    logger.info("Constructing master dataframe")
    master_df = construct_master_df(
        dpdash_db_sources_map=dpdash_sources_variables_map,
        config_file=config_file,
    )

    master_df = make_df_dpdash_ready(df=master_df)

    logger.info("Exporting DataFrame to database")
    db.df_to_table(
        df=master_df,
        schema="forms_derived",
        table_name="dpdash_charts",
        config_file=config_file,
    )

    logger.info("Appending _n_r to non-recruited subjects")
    master_df = append_nr_to_non_recruited_subjects(master_df=master_df)

    logger.info(
        f"Generating DPDash CSVs at {data_root}/*/PHOENIX/PROTECTED/*/processed/*/surveys"
    )
    generate_dpdash_imported_csvs(
        master_df=master_df, data_root=data_root, config_file=config_file
    )

    master_df_dpdash_name = dpdash.get_dpdash_name(
        subject="combined",
        study="AMPSCZ",
        data_type="dpdash",
        category="charts",
        time_range="day1to1",
    )
    master_df_path = Path(output_root) / f"{master_df_dpdash_name}.csv"
    master_df.to_csv(master_df_path, index=False)
    logger.info(f"Exported master dataframe to {master_df_path}")


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    logger.info(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    main(config_file=config_file)
