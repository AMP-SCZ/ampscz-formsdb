#!/usr/bin/env python
"""
Imports JSON drom disk to MongoDB and then exports it to PostgreSQL.
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

import asyncio
import logging

from rich.logging import RichHandler

from formsqc.helpers import utils

MODULE_NAME = "formsqc_importer"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


async def import_jsons() -> None:
    """
    Imports JSONs from disk to MongoDB.
    """
    logger.info("Importing JSONs...")
    cmd = [f"{ROOT}/formsqc/runners/imports/import_jsons.py"]

    proc = await asyncio.create_subprocess_exec(*cmd)

    stdout, stderr = await proc.communicate()
    if stdout:
        logger.info(f"[bold green]stdout:\n{stdout.decode()}")
    if stderr:
        logger.info(f"[bold red]stderr:\n{stderr.decode()}")

    logger.info("Done importing JSONs.")

    return None


async def export_jsons() -> None:
    """
    Exports JSONs from MongoDB to PostgreSQL.
    """
    logger.info("Exporting JSONs...")
    cmd = [f"{ROOT}/formsqc/runners/imports/export_mongo_to_psql.py"]

    proc = await asyncio.create_subprocess_exec(*cmd)

    stdout, stderr = await proc.communicate()
    if stdout:
        logger.info(f"[bold green]stdout:\n{stdout.decode()}")
    if stderr:
        logger.info(f"[bold red]stderr:\n{stderr.decode()}")

    logger.info("Done exporting JSONs.")
    return None


async def import_export_jsons() -> None:
    """
    Imports and exports JSONs from disk to MongoDB and then to PostgreSQL.
    """
    import_jsons_task = import_jsons()

    await import_jsons_task
    await export_jsons()

    return None


async def import_upenn_json() -> None:
    """
    Imports UPenn JSON to MongoDB.
    """
    logger.info("Importing UPenn JSON...")
    cmd = [f"{ROOT}/formsqc/runners/imports/import_upenn_jsons.py"]

    proc = await asyncio.create_subprocess_exec(*cmd)

    stdout, stderr = await proc.communicate()
    if stdout:
        logger.info(f"[bold green]stdout:\n{stdout.decode()}")
    if stderr:
        logger.info(f"[bold red]stderr:\n{stderr.decode()}")

    logger.info("Done importing UPenn JSON.")
    return None


async def export_upenn_json() -> None:
    """
    Export UPenn JSON from MongoDB to PostgreSQL.
    """
    logger.info("Exporting UPenn JSON...")
    cmd = [f"{ROOT}/formsqc/runners/imports/export_upenn_mongo_to_psql.py"]

    proc = await asyncio.create_subprocess_exec(*cmd)

    stdout, stderr = await proc.communicate()
    if stdout:
        logger.info(f"[bold green]stdout:\n{stdout.decode()}")
    if stderr:
        logger.info(f"[bold red]stderr:\n{stderr.decode()}")

    logger.info("Done exporting UPenn JSON.")
    return None


async def import_export_upenn_json() -> None:
    """
    Imports and exports UPenn JSON from disk to MongoDB and then to PostgreSQL.
    """
    import_upenn_json_task = import_upenn_json()

    await import_upenn_json_task
    await export_upenn_json()

    return None


async def import_export() -> None:
    """
    Asynchronously imports and exports JSONs from disk to MongoDB and then to PostgreSQL.
    """
    await asyncio.gather(import_export_jsons(), import_export_upenn_json())


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    logger.info(f"Using config file: {config_file}")

    asyncio.run(import_export())
