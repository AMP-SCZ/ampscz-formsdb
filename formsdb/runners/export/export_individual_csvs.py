#!/usr/bin/env python
"""
Export the individual form level, per subject CSVs.
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
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from rich.logging import RichHandler

from formsdb import constants, data
from formsdb.helpers import cli, dpdash, utils

# Silence pandas UserWarnings
warnings.filterwarnings("ignore", category=UserWarning)

MODULE_NAME = "formsdb.runners.export.export_individual_csvs"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_form_dates(
    form_df: pd.DataFrame,
) -> List[Optional[datetime]]:
    """
    Get the most recent date from the form data.

    Args:
        form_df (pd.DataFrame): The form data.

    Returns:
        List[Optional[datetime]]: The most recent date for each form.
    """
    # drop 'source_mdate' column if it exists
    if "source_mdate" in form_df.columns:
        form_df = form_df.drop(columns=["source_mdate"])

    form_dates: List[Optional[datetime]] = []

    for idx, row in form_df.iterrows():
        row_cols = row.index
        # Check for any columns with 'interview_date' in the name
        date_cols = row_cols[row_cols.str.contains("interview_date", case=False)]

        if len(date_cols) == 0:
            # check for any columns with 'date' in the name
            date_cols = row_cols[row_cols.str.contains("date", case=False)]

        if len(date_cols) == 0:
            form_dates.append(None)
            continue

        found_date_bool = False
        for col in date_cols:
            value = form_df[col].iloc[idx]
            if pd.isna(value):
                continue
            try:
                datetime_value = pd.to_datetime(value)
                datetime_value: datetime = datetime_value.to_pydatetime()

                if not found_date_bool:
                    found_date = datetime_value
                    found_date_bool = True
                elif datetime_value > found_date:
                    found_date = datetime_value
            except ValueError:
                pass

        if found_date_bool:
            # Check if date is after 1950
            if found_date.year > 1950:
                form_dates.append(found_date)
            else:
                form_dates.append(None)
        else:
            form_dates.append(None)

    return form_dates


def export_subject_form_csvs(
    subject_id: str, subject_output_dir: Path, config_file: Path
) -> None:
    """
    Export the forms for a single subject.

    Args:
        subject_id (str): The subject ID.
        subject_output_dir (Path): The output directory for the subject.

    Returns:
        None
    """

    if not subject_output_dir.exists():
        try:
            subject_output_dir.mkdir(parents=True)
            cli.set_permissions(
                path=subject_output_dir, permissions="775", silence_logs=True
            )
        except PermissionError:
            logger.error(f"Permission denied creating: {subject_output_dir}")
            return
    else:
        cli.clear_directory(
            directory=subject_output_dir, pattern="*form*.csv", silence_logs=True
        )

    subject_forms_df = data.get_all_subject_forms(
        config_file=config_file, subject_id=subject_id
    )
    subject_consent_date = data.get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )
    study = subject_id[:2]

    available_forms = subject_forms_df["form_name"].unique()

    for form in available_forms:
        form_df = subject_forms_df[subject_forms_df["form_name"] == form]

        form_df = utils.explode_col(
            df=form_df,
            col="form_data",
        )

        if form == "inclusionexclusion_criteria_review":
            try:
                included = int(form_df["chrcrit_included"].iloc[0])
                form_df["included_excluded"] = included
            except KeyError:
                try:
                    excluded = int(form_df["chrcrit_excluded"].iloc[0])
                    if excluded == 0:
                        form_df["included_excluded"] = 1
                    elif excluded == 1:
                        form_df["included_excluded"] = 0
                except KeyError:
                    continue

        form_dates = get_form_dates(form_df)
        try:
            max_date = max(date for date in form_dates if date is not None)
        except ValueError:
            max_date = None

        for idx, date in enumerate(form_dates):
            if date is not None:
                days_between = dpdash.get_days_between_dates(
                    consent_date=subject_consent_date,
                    event_date=date,
                )
                form_df.loc[idx, "day"] = days_between

        if max_date is None:
            time_range_str = "day1to0001"
        else:
            days_between = dpdash.get_days_between_dates(
                consent_date=subject_consent_date,
                event_date=max_date,
            )
            time_range_str = dpdash.get_time_range(
                consent_date=subject_consent_date,
                event_date=max_date,
            )

            if days_between > 2000:
                time_range_str = "day1to0001"

        form_df["site"] = study

        # rename source_mdate -> mtime
        if "source_mdate" in form_df.columns:
            form_df = form_df.rename(columns={"source_mdate": "mtime"})

        ordered_rows: List[Dict[str, Any]] = []
        for visit in constants.visit_order:
            # check if 'event_name' colume contains visit
            visit_rows = form_df[
                form_df["event_name"].str.contains(f"{visit}_arm", case=False)
            ]
            if visit_rows.empty:
                continue

            ordered_rows.extend(visit_rows.to_dict(orient="records"))  # type: ignore

        if len(ordered_rows) == 0:
            continue
        else:
            form_df = pd.DataFrame(ordered_rows)

        form_df = data.make_df_dpdash_ready(
            df=form_df,
            subject_id=subject_id,
        )
        form_df = utils.clean_df(df=form_df)

        dpdash_name = dpdash.get_dpdash_name(
            study=str(study),
            subject=subject_id,
            time_range=time_range_str,
            data_type="form",
            category=form,
        )

        dpdash_path = f"{subject_output_dir}/{dpdash_name}.csv"
        form_df.to_csv(dpdash_path, index=False)
        cli.set_permissions(
            path=Path(dpdash_path), permissions="664", silence_logs=True
        )


def export_subject_form_wrapper(params: Tuple[str, Path, Path]) -> None:
    """
    Wrapper function to export the forms for a single subject.

    Args:
        params (Tuple[str, Path, Path]): The parameters for the function.

    Returns:
        None
    """
    subject_id, subject_output_dir, config_file = params
    try:
        export_subject_form_csvs(
            subject_id=subject_id,
            subject_output_dir=subject_output_dir,
            config_file=config_file,
        )
    except Exception as e:
        logger.error(f"Error exporting subject {subject_id}: {e}")


def export_csvs(config_file: Path, data_root: Path) -> None:
    """
    Export the individual form level, per subject CSVs.

    Args:
        config_file (str): The path to the config file.

    Returns:
        None
    """

    subject_ids = data.get_all_subjects(config_file=config_file)
    params: List[Tuple[str, Path, Path]] = []

    with utils.get_progress_bar() as progress:
        param_task = progress.add_task(
            "Constructing parameters...", total=len(subject_ids)
        )
        for subject_id in subject_ids:
            subject_network = data.get_subject_network(
                subject_id=subject_id, config_file=config_file
            )
            subject_site = subject_id[:2]

            output_dir = (
                data_root
                / subject_network.capitalize()
                / "PHOENIX"
                / "PROTECTED"
                / f"{subject_network.capitalize()}{subject_site}"
                / "processed"
                / subject_id
                / "surveys"
            )

            params.append((subject_id, output_dir, config_file))
            progress.update(param_task, advance=1)

        with multiprocessing.Pool() as pool:
            subject_task = progress.add_task(
                "Exporting individual CSVs...", total=len(params)
            )
            for _ in pool.imap_unordered(export_subject_form_wrapper, params):
                progress.update(subject_task, advance=1)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    data_root = Path(config_params["data_root"])
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Exporting individual CSVs...")
    export_csvs(config_file=config_file, data_root=data_root)

    logger.info("Done.")
