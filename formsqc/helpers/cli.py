import subprocess
from pathlib import Path
import logging
from typing import Optional


def clear_directory(
    directory: Path, logger: Optional[logging.Logger] = None, pattern: str = "*"
):
    """
    Clears the contents of a directory.

    Parameters
    ----------
    directory : Path
        The directory to clear.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    files_deleted = 0

    for file in directory.glob(pattern):
        if file.is_dir():
            logger.warning(f"Removing directory {file}")
            clear_directory(file)
        else:
            files_deleted += 1
            try:
                file.unlink()
            except FileNotFoundError:
                logger.warning(f"File {file} not found. Skipping.")

    logger.info(f"Deleted {files_deleted} files from {directory}")


def get_repo_root() -> Path:
    """
    Returns the root directory of the current Git repository.

    Uses the command `git rev-parse --show-toplevel` to get the root directory.

    Returns
    -------
    Path
        The root directory of the current Git repository.
    """
    repo_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
    repo_root = repo_root.decode("utf-8").strip()
    return Path(repo_root)
