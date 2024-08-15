# Transform

For more general details check the [Data Team handbook](https://about.gitlab.com/handbook/business-technology/data-team/). More specific `DBT` guideline is shown in [dbt guide](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/).

The module contains all modulus accomplishing the transformation part and loading data in the `PREP` and `PROD` layer of the Data Warehouse.

## Scripts & Modules

### dbt_dependecy_check
This module returns a csv with downstream dbt dependencies and data science exposures for a list of model provided by the user. The module run the `dbt ls` command to find these dependencies and can accept multiple models in a single run.

Steps to use:
1. Navigate to dbt instance (`jump analytics`, `make run-dbt`)
2. Search for `models_to_check.csv` under the `snowflake-dbt` directory
3. Add a list of model names to check, one name per line in the csv
    Example list:
    ```
    zendesk_tickets_xf
    fct_event
    ```
4. From terminal, run `python3 dbt_dependency_check.py`
5. Search for `models_with_dependencies.csv` to see output with the following fields:
    - `model_name`: Name of model added to `models_to_check.csv`
    - `dbt_model_dependencies`: List of downstream dbt dependencies, including the `model_name` tested
    - `dbt_model_dependencies_count`: Count of total dbt dependencies. The minimum result is `1` because the `model_name` tested is considered a dependency.
    - `ds_exposure_dependencies`: List of downstream data science exposures associated with the `model_name`
    - `ds_exposure_dependencies_count`: Count of total downstream data science exposures associated with the `model_name`