More information on this extraction is in the [handbook](https://about.gitlab.com/handbook/business-technology/data-team/platform/pipelines/#level-up--thought-industries-extract)


### One-time Setup of Snowflake Database Environment
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

CREATE OR REPLACE TABLE code_redemptions (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE users (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE content (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE meetings (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE clients (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE assessment_attempts (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE coupons (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);
```

### One-time Setup of Metadata Database Environment

The Metadata database is a GCP CloudSQL database used to store the cursor state for each cursor-based endpoint.

These are the initial steps to set it up:

```sql
CREATE DATABASE level_up_metadata;

-- Switch to the database
\c level_up_metadata;

-- Create the schema
CREATE SCHEMA prod_metadata;
CREATE TABLE prod_metadata.cursor_state (
    id SERIAL PRIMARY KEY,
    endpoint varchar (255) NOT NULL,
    cursor_id varchar (255) NOT NULL,
    uploaded_at timestamp DEFAULT CURRENT_TIMESTAMP
  );

CREATE SCHEMA test_metadata;
CREATE TABLE test_metadata.cursor_state (
    id SERIAL PRIMARY KEY,
    endpoint varchar (255) NOT NULL,
    cursor_id varchar (255) NOT NULL,
    uploaded_at timestamp DEFAULT CURRENT_TIMESTAMP
  );
```
