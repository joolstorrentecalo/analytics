with source as (  
  SELECT *
  FROM  {{ source('elastic_billing', 'itemized_costs') }}
),

parsed as (
SELECT 
source.extraction_start_date,
source.extraction_end_date,
resources.value['period']['start']::TIMESTAMP as resource_start_date,
resources.value['period']['end']::TIMESTAMP as resource_end_date,
resources.value['hours']::float as hours,
resources.value['instance_count']::NUMBER as instance_count,
resources.value['kind']::varchar as kind,
resources.value['price']::varchar as cost,
resources.value['name']::varchar as name,
resources.value['price_per_hour']::float as price_per_hour,
resources.value['sku']::varchar as sku
FROM source
    INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(payload), outer => TRUE) AS dims
    INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(dims.value), outer => TRUE, mode => 'ARRAY') AS resources
WHERE dims.path = 'resources'
order by 1 desc)


SELECT * FROM parsed

