#!/usr/bin/env python
"""
Run all compute scripts in the formsdb/runners/compute directory.
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

from rich.logging import RichHandler

from formsdb.helpers import utils, cli

MODULE_NAME = "formsdb.scripts.compute"

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
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    config_params = utils.config(config_file, "general")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    repo_root = cli.get_repo_root()
    target_dir = repo_root / "formsdb" / "runners" / "compute"
    targets = target_dir.glob("compute_*.py")
    targets = sorted(targets)

    targets = [target for target in targets if target not in [Path(__file__)]]

    logger.info(f"Founds {len(list(targets))} compute scripts", extra={"markup": True})
    logger.info("Modules found:", extra={"markup": True})
    for target in targets:
        logger.info(f"  - {target.name}", extra={"markup": True})

    logger.info("[green]Running compute scripts", extra={"markup": True})

    for target in targets:
        if target == Path(__file__):
            continue

        console.print(f"Running {target.name}")
        exec(open(target, encoding="utf-8").read())
