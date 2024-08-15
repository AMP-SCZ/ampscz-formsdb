#!/usr/bin/env python
"""
Wipes all assesment Day data from DPDash
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


import argparse
import yaml
import requests

from formsdb.helpers import utils

MODULE_NAME = "formsdb.scripts.wipe_dpdash"

console = utils.get_console()


class ImporterApiService:
    def __init__(
        self,
        username: str,
        api_key: str,
    ):
        credentials = {
            "x-api-user": username,
            "x-api-key": api_key,
        }
        self.headers = {"content-type": "application/json", **credentials}

    def routes(self, url, path):
        routes = {"metadata": url + "metadata", "day_data": url + "day"}

        return routes[path]

    def get(self, url: str) -> requests.Response:
        response = requests.get(url, headers=self.headers)
        return response

    def delete(self, url: str) -> requests.Response:
        response = requests.delete(url, headers=self.headers)
        return response


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    parser = argparse.ArgumentParser(description="Wipe DPDash data")
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="Path to the configuration file",
    )

    args = parser.parse_args()

    config_file = Path(args.config)

    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)

    username = config["api_user"]
    api_key = config["api_key"]

    api = ImporterApiService(
        username=username,
        api_key=api_key,
    )

    response = api.delete("https://staging.dpdash.itpmclean.org/api/v1/import/data/day")
