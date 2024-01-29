import subprocess
from pathlib import Path
import logging
from typing import Optional


def clear_directory(directory: Path, logger: Optional[logging.Logger] = None) -> None:
    """
    Clears the contents of a directory.

    Parameters
    ----------
    directory : Path
        The directory to clear.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    for file in directory.iterdir():
        if file.is_dir():
            logging.warning(f"Removing directory {file}")
            clear_directory(file)
        else:
            file.unlink()


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
