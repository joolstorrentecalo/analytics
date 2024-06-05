{% docs dedupe_aws_source %}

## Dedupe AWS Source

This macro is designed to deduplicate data from an AWS billing source table. It works on incremental models and uses a specified unique key and condition column to identify the most recent records.

## Fields:

* source_table: (Required) The name of the source table in your AWS billing schema.
* source_schema: (Optional, default: 'aws_billing') The schema containing the source table.
* condition_column: (Optional, default: 'metadata$file_last_modified') The column used to determine the latest record for each unique key.
* unique_key: (Optional, default: 'id') The column that uniquely identifies each record.

**Example Usage:**

{% raw %}
```jinja2
{% set my_deduped_table = dedupe_aws_source('my_cost_usage_data') %}
select * from {{ my_deduped_table }}
```
{% endraw %}
## Generated SQL:

{% raw %}
```sql
SELECT *
FROM (
SELECT *,
    metadata$file_last_modified AS modified_at_,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY metadata$file_last_modified DESC) AS rn
FROM "RAW".aws_billing.my_cost_usage_data
WHERE metadata$file_last_modified > (SELECT MAX(modified_at) FROM my_deduped_table)
) subquery
WHERE rn = 1
```
{% endraw %}
{% enddocs %}

{% docs dedupe_and_union_aws_source %}

## Deduplicating and Unioning Multiple AWS Sources

`dedupe_and_union_aws_source`

This macro combines data from multiple AWS billing source tables, deduplicates records within each table, and then unions the results. It leverages the `dedupe_aws_source` macro for deduplication.

## Fields:

* `source_tables` (**required**): A list of the names of your AWS source tables (e.g., `['table1', 'table2', 'table3']`).
* `schema` (optional, default: 'aws_billing'): The schema where your source tables are located.
* `condition_column` (optional, default: 'metadata$file_last_modified'): The column used to determine the most recent record (usually a timestamp).
* `unique_key` (optional, default: 'id'): A comma-separated string specifying the columns that uniquely identify a record (e.g., "value['field1']::VARCHAR, value['field2']::VARCHAR").

**Example Usage:**
{% raw %}
```jinja2
{% set unique_key = "value['bill_payer_account_id']::VARCHAR, 
    value['bill_invoice_id']::VARCHAR, 
    value['identity_line_item_id']::VARCHAR, 
    value['identity_time_interval']::VARCHAR" %}
{% set source_tables = ['dedicated_legacy_0475', 
    'dedicated_dev_3675'] %}


WITH all_raw_deduped as (
{{ dedupe_and_union_aws_source(source_tables, 
    'aws_billing', 
    'metadata$file_last_modified', 
    unique_key) }}
)

```
{% endraw %}
## Generated SQL:
{% raw %}
```sql
WITH all_raw_deduped as (     
SELECT *
FROM (
SELECT *,
    metadata$file_last_modified AS modified_at_,
    ROW_NUMBER() OVER (PARTITION BY value['bill_payer_account_id']::VARCHAR, 
    value['bill_invoice_id']::VARCHAR, 
    value['identity_line_item_id']::VARCHAR, 
    value['identity_time_interval']::VARCHAR ORDER BY metadata$file_last_modified DESC) AS rn
FROM "RAW".aws_billing.dedicated_legacy_0475
WHERE metadata$file_last_modified > (SELECT MAX(modified_at) FROM "PREP".aws_billing.aws_billing_source_test)
) subquery
WHERE rn = 1
        UNION ALL   
SELECT *
FROM (
SELECT *,
    metadata$file_last_modified AS modified_at_,
    ROW_NUMBER() OVER (PARTITION BY value['bill_payer_account_id']::VARCHAR, 
    value['bill_invoice_id']::VARCHAR, 
    value['identity_line_item_id']::VARCHAR, 
    value['identity_time_interval']::VARCHAR ORDER BY metadata$file_last_modified DESC) AS rn
FROM "RAW".aws_billing.dedicated_dev_3675
WHERE metadata$file_last_modified > (SELECT MAX(modified_at) FROM "PREP".aws_billing.aws_billing_source_test)
) subquery
WHERE rn = 1
```
{% endraw %}
{% enddocs %}