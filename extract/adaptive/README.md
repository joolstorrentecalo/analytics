### setting up env
1. Set-up the necessary schemas/tables in the raw database in Snowflake:
    ```sql
    use <raw>.clari;
    use role loader;
    create schema adaptive_custom;

    create table processed_versions (
        version varchar,
        processed_at timestamp
    );
    ```
1. Add the following Kubernetes secrets:
    - `ADAPTIVE_USERNAME`
    - `ADAPTIVE_PASSWORD`
        
