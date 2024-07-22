Kantata data pipeline information is in this [internal handbook page](https://gitlab.com/gitlab-com/content-sites/internal-handbook/-/blob/main/content/handbook/enterprise-data/platform/pipelines/_index.md?ref_type=heads#kantata).

To set-up Snowflake environment:

```sql
use role loader;
use database "RAW";

create schema kantata;

CREATE FILE FORMAT kantata.kantata_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
COMPRESSION = 'GZIP';

CREATE STAGE kantata.kantata_csv_stage
FILE_FORMAT = kantata_csv_format;

-- Snowflake tables are automatically created by the python code
```
