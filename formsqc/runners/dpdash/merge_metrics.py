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
    if parent.name == "ampscz-formsqc":
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

from formsqc.helpers import cli, utils, dpdash

MODULE_NAME = "formsqc.runners.dpdash.merge_metrics"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_dpdash_sources_variables_map(config_file: Path) -> Dict[str, List[str]]:
    """
    Get the mapping of sources to variables from the dpdash config file.

    Args:
        config_file (Path): Path to the dpdash config file.

    Returns:
        Dict[str, List[str]]: Mapping of sources to variables.
    """

    dpdash_sources = utils.config(path=config_file, section="dpdash-sources")
    dpdash_variables = utils.config(path=config_file, section="dpdash-variables")

    dpdash_sources_variables_map = {}

    for source_name, source_pattern in dpdash_sources.items():
        source_variables = dpdash_variables.get(source_name)
        variables = source_variables.split(",") if source_variables else []

        # strip whitespace from variable names
        variables = [variable.strip() for variable in variables]

        dpdash_sources_variables_map[source_pattern] = variables

    return dpdash_sources_variables_map


def construct_master_df(
    data_root: Path, dpdash_sources_variables_map: Dict[str, List[str]]
) -> pd.DataFrame:
    """
    Construct a master dataframe by combining data from all dpdash sources.

    Args:
        data_root (Path): Root directory containing the dpdash data.
        dpdash_sources_variables_map (Dict[str, List[str]]): Mapping of sources to variables.

    Returns:
        pd.DataFrame: Master dataframe.
    """
    all_variables = []
    for source, variables in dpdash_sources_variables_map.items():
        all_variables.extend(variables)

    master_df = pd.DataFrame()

    with utils.get_progress_bar() as progress:
        sources_task = progress.add_task(
            "Processing sources", total=len(dpdash_sources_variables_map)
        )
        for source, variables in dpdash_sources_variables_map.items():
            progress.update(
                sources_task, advance=1, description=f"Processing source: {source}"
            )

            source_files = list(data_root.glob(source))

            logger.debug(f"source: {source}: variables: {variables}")
            logger.info(f"Processing {source} with {len(source_files)} files")

            source_task = progress.add_task(
                f"Processing {source}", total=len(source_files)
            )
            for file in source_files:
                progress.update(
                    source_task,
                    advance=1,
                    description=f"Processing {source}: {file.name}",
                )
                dpdash_dict = dpdash.parse_dpdash_name(file.name)
                subject_id = dpdash_dict["subject"]

                try:
                    temp_df = pd.read_csv(file)
                    for var in variables:
                        try:
                            var_value = temp_df[var].values[0]
                            master_df.loc[subject_id, var] = var_value  # type: ignore
                        except KeyError:
                            pass
                except FileNotFoundError:
                    pass

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
    df = df.reset_index()
    df = df.rename(columns={"index": "subject_id"})

    required_columns = ["reftime", "day", "timeofday", "weekday"]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    df["day"] = 1

    return df


def generate_dpdash_imported_csvs(master_df: pd.DataFrame, output_root: Path) -> None:
    """
    Generate the DPDash imported csvs.

    Args:
        master_df (pd.DataFrame): Master dataframe.
        output_root (Path): Output directory.
    """

    logger.info(f"Clearing data from {output_root}")
    cli.clear_directory(directory=output_root, pattern="*dpdash_charts*.csv")

    logger.info(f"Exporting data to {output_root}")
    with utils.get_progress_bar() as progress:
        export_task = progress.add_task("Exporting data", total=len(master_df))
        for _, row in master_df.iterrows():
            progress.update(
                export_task,
                advance=1,
                description=f"Exporting data for {row['subject_id']}",
            )
            subject_id = row["subject_id"]

            site = subject_id[:2]

            dpdash_name = dpdash.get_dpdash_name(
                subject=subject_id,
                study=site,
                data_type="form",
                category="dpdash",
                optional_tag=["charts"],
                time_range="day1to1",
            )

            export_path = Path(output_root) / f"{dpdash_name}.csv"

            df_part = row.to_frame().T
            df_part.to_csv(export_path, index=False)

    logger.info(f"Exported {len(master_df)} files to {output_root}")


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

    dpdash_sources_variables_map = get_dpdash_sources_variables_map(config_file)
    logger.debug(f"dpdash_sources_variables_map: {dpdash_sources_variables_map}")

    logger.info("Constructing master dataframe")
    master_df = construct_master_df(
        data_root=data_root, dpdash_sources_variables_map=dpdash_sources_variables_map
    )

    master_df = make_df_dpdash_ready(df=master_df)

    generate_dpdash_imported_csvs(master_df=master_df, output_root=output_root)

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
