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

from rich.logging import RichHandler

from formsqc.helpers import utils

MODULE_NAME = "formsqc"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

if __name__ == "__main__":
    console.rule("[bold red]formsqc[/bold red]")
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <SubjectID> <Network> <OutputDir")
        sys.exit(1)

    subject = sys.argv[1]
    network = sys.argv[2]
    output_dir = sys.argv[3]

    site_acronym = subject[0:2]

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    data_root = Path(config_params["data_root"])

    subject_json = utils.get_subject_json(subject, network, data_root)
    console.print(f"Using subject json: {subject_json}")
