WITH events AS (

	 SELECT *
	 FROM {{ref('snowplow_unnested_events')}}
     WHERE is_staging_url = FALSE

)

SELECT event_id 	AS root_id,
	   web_page_id 	AS id
FROM events
