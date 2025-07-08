#!/usr/bin/env python
"""
Import Form QC determinations into Postgres
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
from typing import List, Optional, Tuple, Dict, Any

import pandas as pd
from rich.logging import RichHandler

from formsdb.helpers import db, utils
from formsdb import constants

MODULE_NAME = "formsdb.runners.imports.import_form_qc"
console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def split_variable(variable: str, visits: List[str]) -> Tuple[str, Optional[str]]:
    """
    Split a variable name into its base name and visit suffix.

    Examples:
    >>> split_variable("scid5_schizotypal_personality_sciddpq_screening", ["screening", "visit2"])
    ("scid5_schizotypal_personality_sciddpq", "screening")
    >>> split_variable("varname", ["visit1", "visit2"])
    ("varname", None)

    Args:
        variable (str): The variable name to split.
        visits (List[str]): A list of visit suffixes to match.

    Returns:
        Tuple[str, Optional[str]]: A tuple containing the base variable name and 
            the matched visit suffix.
            If no visit suffix is found, the second element will be None.
    """
    # Sort visits by length in descending order to match longer visits first
    for visit in sorted(visits, key=lambda x: -len(x)):
        suffix = f"_{visit}"
        if variable.endswith(suffix):
            base = variable[: -len(suffix)]
            return (base, visit)
    return (variable, None)  # Return None if no visit match is found


def extract_qc_records(
    qc_root: Path
) -> pd.DataFrame:
    """
    Extract QC records from Excel files in the QC Root directory.

    These files are generated exteranlly.

    Args:
        qc_root (Path): Path to the directory containing QC Excel files.
    Returns:
        pd.DataFrame: DataFrame containing the extracted QC records.
    """
    logger.info(f"Looking for QC files in {qc_root}")
    qc_files = list(Path(qc_root).glob("*.xlsx"))
    logger.info(f"Found {len(qc_files)} QC files.")

    visit_order = constants.visit_order
    form_names = constants.form_name_to_abbrv.keys()

    results: List[Dict[str, Any]] = []

    for qc_file in qc_files:
        file_results: List[Dict[str, Any]] = []
        logger.info(f"Processing file: {qc_file}")
        df = pd.read_excel(qc_file, engine="openpyxl")

        for _, row in df.iterrows():
            subject_id = row["subject"]

            for event in visit_order:
                for form in form_names:
                    col = f"{form}_{event}"

                    try:
                        value = row[col]
                    except KeyError:
                        value = "NA"

                    if pd.isna(value):
                        expected = True
                        completed = True
                        qc_issue = False
                    elif value == "NA":
                        expected = False
                        completed = False
                        qc_issue = False
                    elif value == "Not in CSV":
                        expected = True
                        completed = False
                        qc_issue = False
                    elif value == "Not Marked Complete":
                        expected = True
                        completed = False
                        qc_issue = False
                    else:
                        expected = True
                        completed = None
                        qc_issue = True

                    file_results.append(
                        {
                            "subject_id": subject_id,
                            "event_name": event,
                            "form_name": form,
                            "expected": expected,
                            "completed": completed,
                            "qc_issue": qc_issue,
                            "comment": value,
                        }
                    )

        results.extend(file_results)

    df = pd.DataFrame(results)
    return df


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    data_params = utils.config(path=config_file, section="data")
    form_qc_root = Path(data_params["form_qc_root"])

    form_qc_df = extract_qc_records(qc_root=form_qc_root)

    logger.info("Inserting form QC data into database")
    db.df_to_table(
        config_file=config_file,
        df=form_qc_df,
        table_name="form_qc",
        schema="forms_derived",
        if_exists="replace",
    )
    logger.info("Done!")
