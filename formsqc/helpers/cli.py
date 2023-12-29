import subprocess
from pathlib import Path


def clear_directory(directory: Path) -> None:
    """
    Clears the contents of a directory.

    Parameters
    ----------
    directory : Path
        The directory to clear.
    """
    for file in directory.iterdir():
        file.unlink()


def get_repo_root() -> str:
    """
    Returns the root directory of the current Git repository.

    Uses the command `git rev-parse --show-toplevel` to get the root directory.
    """
    repo_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
    repo_root = repo_root.decode("utf-8").strip()
    return repo_root
