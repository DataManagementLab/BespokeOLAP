import subprocess
import logging

logger = logging.getLogger(__name__)


def has_passwordless_sudo(logger=None):
    """
    Returns True if sudo can run without prompting for a password.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        subprocess.run(
            ["sudo", "-n", "true"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def set_hugepages(node=0, page_kb=2048, count=0):
    """
    Set hugepages using sudo without a password prompt.

    Requires a sudoers rule like:
      username ALL=(root) NOPASSWD: /usr/bin/tee /sys/devices/system/node/node*/hugepages/*/nr_hugepages
    """

    if not has_passwordless_sudo(logger):
        logger.warning("Passwordless sudo is not available. Not setting hugepages.")
        return False

    path = (
        f"/sys/devices/system/node/node{node}/hugepages/"
        f"hugepages-{page_kb}kB/nr_hugepages"
    )

    subprocess.run(
        ["sudo", "/usr/bin/tee", path],
        input=f"{count}\n",
        text=True,
        stdout=subprocess.DEVNULL,  # suppress tee echo
        stderr=subprocess.PIPE,
        check=True,
    )
