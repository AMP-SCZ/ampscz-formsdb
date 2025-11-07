"""
Helper functions for interacting with Airflow API.
"""

import logging
import socket

import requests

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_airflow_access_token(
    hostname: str,
    username: str,
    password: str,
    timeout: int = 10,
):
    """
    Get an access token from Airflow API.

    params:
        hostname: Airflow hostname.
        username: Airflow username.
        password: Airflow password.

    returns:
        Access token string if successful, None otherwise.
    """
    url = f"{hostname}/auth/token"
    headers = {
        "Content-Type": "application/json",
    }
    data = {
        "username": username,
        "password": password,
    }
    response = requests.post(
        url,
        headers=headers,
        json=data,
        verify=False,
        timeout=timeout,
    )
    if response.status_code == 201:
        token = response.json().get("access_token")
        return token
    else:
        logger.error(
            f"Failed to get access token. Status code: {response.status_code}, Response: {response.text}"
        )
        return None


def trigger_airflow_imported_asset_event(
    hostname: str,
    username: str,
    password: str,
    imported_asset_id: int,
    timeout: int = 10,
):
    """
    Trigger an Airflow event for an imported asset.

    params:
        hostname: Airflow hostname.
        username: Airflow username.
        password: Airflow password.
        imported_asset_id: ID of the imported asset.

    returns:
        None
    """
    access_token = get_airflow_access_token(
        hostname=hostname,
        username=username,
        password=password,
    )
    if not access_token:
        print("Cannot trigger asset event without access token.")
        return
    url = f"{hostname}/api/v2/assets/events"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    data = {"asset_id": imported_asset_id, "extra": {"source": socket.gethostname()}}
    response = requests.post(
        url,
        headers=headers,
        json=data,
        verify=False,
        timeout=timeout,
    )
    if response.status_code == 200:
        logger.info("Successfully triggered asset event for PNL imported asset.")
    else:
        logger.error(
            f"Failed to trigger asset event. Status code: {response.status_code}, "
            f"Response: {response.text}"
        )
