"""
Module providing command line interface for the pipeline.
"""

import logging
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


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

    logger.info(
        f"Deleted {files_deleted} files from {directory} matching pattern '{pattern}'."
    )


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


def execute_commands(
    command_array: list,
    shell: bool = False,
    on_fail: Callable = lambda: sys.exit(1),
) -> subprocess.CompletedProcess:
    """
    Executes a command and returns the result.

    Args:
        command_array (list): The command to execute as a list of strings.
        shell (bool, optional): Whether to execute the command in a shell. Defaults to False.
        logger (Optional[logging.Logger], optional): The logger to use for logging.
            Defaults to None.
        on_fail (Callable, optional): The function to call if the command fails.
            Defaults to lambda: sys.exit(1).

    Returns:
        subprocess.CompletedProcess: The result of the command execution.

    """
    logger.debug("Executing command:")
    # cast to str to avoid error when command_array is a list of Path objects
    command_array = [str(x) for x in command_array]

    if logger:
        logger.debug(" ".join(command_array))

    if shell:
        result = subprocess.run(
            " ".join(command_array),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            check=False,
        )
    else:
        result = subprocess.run(
            command_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
        )

    if result.returncode != 0:
        logger.error("=====================================")
        logger.error("Command: %s", " ".join(command_array))
        logger.error("=====================================")
        logger.error("stdout:")
        logger.error(result.stdout.decode("utf-8"))
        logger.error("=====================================")
        logger.error("stderr:")
        logger.error(result.stderr.decode("utf-8"))
        logger.error("=====================================")
        logger.error("Exit code: %s", str(result.returncode))
        logger.error("=====================================")

        if on_fail:
            on_fail()

    return result


def send_email(
    subject: str,
    message: str,
    recipients: List[str],
    sender: str,
    attachments: Optional[List[Path]] = None,
) -> None:
    """
    Send an email with the given subject and message to the given recipients.

    Uses the `mail` binary to send the email.

    Args:
        subject (str): The subject of the email.
        message (str): The message of the email.
        recipients (List[str]): The recipients of the email.
        sender (str): The sender of the email.
        attachments (List[Path], optional): The attachments to add to the email.
            Defaults to None.

    Returns:
        None
    """
    if shutil.which("mail") is None:
        logger.error("[red][u]mail[/u] binary not found.[/red]", extra={"markup": True})
        logger.warning(
            "[yellow]Skipping sending email.[/yellow]", extra={"markup": True}
        )
        return

    host_name = socket.getfqdn()

    with tempfile.NamedTemporaryFile(mode="w", prefix="email_", suffix=".eml") as temp:
        temp.write(f"From: {sender}\n")
        temp.write(f"To: {', '.join(recipients)}\n")
        temp.write(f"X-Mailer: formsdb-cli @ {host_name}\n")
        temp.write(f"Subject: {subject}\n")
        temp.write("\n")
        temp.write(message)
        temp.write("\n")
        if attachments is not None:
            temp.write("\n")
            temp.write(f"{len(attachments)} Attachment(s):\n")
            for attachment in attachments:
                temp.write(str(attachment.name) + "\n")
        temp.flush()

        command_array = [
            "mail",
            "-s",
            f"'{subject}'",  # wrap subject in quotes to avoid issues with special characters
        ]

        if attachments is not None:
            for attachment in attachments:
                command_array += ["-a", str(attachment)]

        command_array += recipients

        command_array += ["<", temp.name]

        logger.debug("Sending email:")
        logger.debug(" ".join(command_array))
        execute_commands(command_array, shell=True)
