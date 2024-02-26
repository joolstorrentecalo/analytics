Please see [handbook (internal only)](https://gitlab.com/gitlab-com/content-sites/internal-handbook/-/blob/main/content/handbook/enterprise-data/platform/pipelines/_index.md?ref_type=heads#pajamas-adoption-scanner) for details on this extraction.

Below are instructions to set-up the proper Snowflake environment.

### One-time Setup of Database Environment
#### Create Stage Command
```sql
use role loader;

create schema raw.pajamas_adoption_scanner;

use raw.pajamas_adoption_scanner;

CREATE STAGE pajamas_adoption_scanner_load
FILE_FORMAT = (TYPE = 'JSON');
```

#### Create Table Command
Execute following command for creating new table in RAW database
```sql
CREATE OR REPLACE TABLE raw.pajamas_adoption_scanner.adoption_by_group (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);
```

