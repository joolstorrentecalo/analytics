{{ config({
        "materialized": "incremental",
        "unique_key": "mart_available_to_renew_new_snapshot_id",
        "tags": ["edm_snapshot", "atr_snapshots"]
    })
}}

WITH snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2020-03-01'
      AND date_actual <= CURRENT_DATE {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
      AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

    {% endif %}

), mart_available_to_renew_new AS (

    SELECT *
    FROM {{ ref('prep_mart_available_to_renew_new_snapshot_base') }}

), mart_available_to_renew_new_spined AS (

    SELECT
      snapshot_dates.date_id     AS snapshot_id,
      snapshot_dates.date_actual AS snapshot_date,
      mart_available_to_renew_new.*
    FROM mart_available_to_renew_new
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= mart_available_to_renew_new.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('mart_available_to_renew_new.dbt_valid_to') }}

), final AS (

     SELECT
       {{ dbt_utils.generate_surrogate_key(['snapshot_id', 'primary_key']) }} AS mart_available_to_renew_new_snapshot_id,
       *
     FROM mart_available_to_renew_new_spined

)

SELECT *
FROM final
