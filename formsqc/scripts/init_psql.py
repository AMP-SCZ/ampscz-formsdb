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


import json
from typing import Set

from formsqc.helpers import db, utils

console = utils.get_console()


def create_tables(config_file: Path) -> None:
    create_queries = [
        """
        CREATE TABLE network (
            id VARCHAR(25) PRIMARY KEY
        );
        """,
        """
        CREATE TABLE site (
            id VARCHAR(25) PRIMARY KEY,
            site_name VARCHAR(255) NOT NULL,
            site_country VARCHAR(255),
            network_id VARCHAR(25) REFERENCES network(id)
        );
        """,
        """
        CREATE TABLE subjects (
            id VARCHAR(25) PRIMARY KEY,
            site_id VARCHAR(25) REFERENCES site(id)
        );
        """,
        """
        CREATE TABLE forms (
            subject_id VARCHAR(25) REFERENCES subjects(id),
            form_name VARCHAR(255) NOT NULL,
            event_name VARCHAR(255) NOT NULL,
            form_data JSONB NOT NULL,
            source_mdate TIMESTAMP NOT NULL,
            variables_with_data INT NOT NULL,
            variables_without_data INT,
            total_variables INT,
            percent_complete FLOAT,
            PRIMARY KEY (subject_id, form_name, event_name)
        );
        """,
        """
        CREATE TABLE upenn_forms (
            subject_id VARCHAR(25) REFERENCES subjects(id),
            event_name VARCHAR(255) NOT NULL,
            event_type VARCHAR(255) NOT NULL,
            form_data JSONB NOT NULL,
            source_mdate TIMESTAMP NOT NULL,
            PRIMARY KEY (subject_id, event_name, event_type)
        );
        """,
        """
        CREATE TABLE subject_visit_status (
            subject_id VARCHAR(25) REFERENCES subjects(id),
            subject_visit_status VARCHAR(255) NOT NULL,
            PRIMARY KEY (subject_id)
        );
        """,
        """
        CREATE TABLE subject_converted (
            subject_id VARCHAR(25) REFERENCES subjects(id),
            subject_converted BOOLEAN NOT NULL,
            subject_converted_event VARCHAR(255),
            PRIMARY KEY (subject_id)
        );
        """,
        """
        CREATE TABLE subject_removed (
            subject_id VARCHAR(25) REFERENCES subjects(id),
            subject_removed BOOLEAN NOT NULL,
            subject_removed_event VARCHAR(255),
            subject_removed_reason VARCHAR(255),
            PRIMARY KEY (subject_id)
        );
        """,
    ]

    commands = create_queries

    db.execute_queries(
        config_file=config_file,
        queries=commands,
    )


def populate_sites(sites_json: Path, config_file: Path) -> None:
    """
    Populates the site table with data from a JSON file.

    Args:
        sites_json (str): The path to the JSON file containing the site data.
        config_file (str): The path to the configuration file containing the connection parameters.
    """

    class Site:
        def __init__(self, id, name, country, network_id):
            self.id = id
            self.name = name
            self.country = country
            self.network_id = network_id

            if self.country == "":
                self.country = "NULL"

    with open(sites_json) as f:
        sites = json.load(f)

    networks: Set[str] = set()
    commands = []

    for site in sites:
        site_obj = Site(
            id=db.santize_string(site["id"]),
            name=db.santize_string(site["name"]),
            country=db.santize_string(site["country"]),
            network_id=db.santize_string(site["network"]),
        )

        networks.add(site_obj.network_id)

        command = f"""
        INSERT INTO site (id, site_name, site_country, network_id)
        VALUES ('{site_obj.id}', '{site_obj.name}', '{site_obj.country}', '{site_obj.network_id}');
        """

        command = db.handle_null(command)

        commands.append(command)

    for network in networks:
        command = f"""
        INSERT INTO network (id)
        VALUES ('{network}');
        """

        command = db.handle_null(command)

        # Add the command to the list's beginning so that it is executed first
        commands.insert(0, command)

    console.log(f"Found {len(networks)} Networks")

    db.execute_queries(
        config_file=config_file,
        queries=commands,
        show_commands=False,
        silent=True,
    )


if __name__ == "__main__":
    console.rule("[bold red]Initializing PostgreSQL database")
    config_file = utils.get_config_file_path()
    console.log(f"Using config file: {config_file}")
    params = utils.config(config_file, "general")

    console.log("Creating tables...")
    create_tables(config_file)

    console.log("Populating sites...")
    sites_json = Path(params["sites_json"])
    console.log(f"Using sites JSON file: {sites_json}")
    populate_sites(sites_json, config_file)
