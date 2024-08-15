More information on this extraction is in the [handbook](https://about.gitlab.com/handbook/business-technology/data-team/platform/pipelines/#level-up--thought-industries-extract)


### One-time Setup of Database Environment
```sql
use role loader;
use database <raw_db>;

-- set-up stage
create schema level_up;
use schema level_up;

CREATE STAGE level_up_load_stage
FILE_FORMAT = (TYPE = 'JSON');


-- create tables
CREATE OR REPLACE TABLE course_completions (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);


CREATE OR REPLACE TABLE logins (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE visits (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE course_views (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE course_actions (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE course_purchases (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE learning_path_actions (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE email_captures (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE awards (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

```
