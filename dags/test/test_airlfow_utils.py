""" This file contains common operators/functions to be used across multiple DAGs """
import os
import urllib.parse
import pathlib
from datetime import date, timedelta
from typing import List, Dict


SSH_REPO = "git@gitlab.com:gitlab-data/analytics.git"
HTTP_REPO = "https://gitlab.com/gitlab-data/analytics.git"
DATA_IMAGE = "registry.gitlab.com/gitlab-data/data-image/data-image:v1.0.27"
DBT_IMAGE = "registry.gitlab.com/gitlab-data/dbt-image:v0.0.1"
PERMIFROST_IMAGE = "registry.gitlab.com/gitlab-data/permifrost:v0.13.1"
ANALYST_IMAGE = "registry.gitlab.com/gitlab-data/analyst-image:v0.0.2"

SALES_ANALYTICS_NOTEBOOKS_PATH = (
    "/Users/nfiguera/repos/analytics/sales_analytics_notebooks/"
)


def get_sales_analytics_notebooks(frequency: str) -> Dict:
    notebooks = []
    fileNames = []

    path = pathlib.Path(f"{SALES_ANALYTICS_NOTEBOOKS_PATH}/{frequency}/")

    n = 0
    for file in path.rglob("*.ipynb"):
        print(n + 1)
        relative_path = file.relative_to(SALES_ANALYTICS_NOTEBOOKS_PATH)
        notebooks.append(relative_path.as_posix())
        expanded_name = (
            str(relative_path.parent).replace("/", "_") + "_" + relative_path.stem
        )
        fileNames.append(expanded_name)

    return dict(zip(notebooks, fileNames))


def test_cd_into_local_path():
    notebooks = get_sales_analytics_notebooks(frequency="daily")
    print(notebooks)
    for notebook, task_name in notebooks.items():
        if notebook is None:
            break

        absolute_path = pathlib.Path(SALES_ANALYTICS_NOTEBOOKS_PATH) / notebook
        notebook_parent = absolute_path.parent.as_posix()
        notebook_filename = absolute_path.name
        print(notebook_parent)
        print(notebook_filename)


test_cd_into_local_path()
