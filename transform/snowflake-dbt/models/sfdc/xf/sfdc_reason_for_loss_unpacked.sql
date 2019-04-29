{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}


WITH opportunities AS (

	SELECT *
	FROM {{ref('sfdc_opportunity_xf')}}

),

flatten AS (

	SELECT 	opportunities.*,
			reasons.value :: STRING AS reason_for_loss_unpacked
	FROM opportunities,
	LATERAL flatten(input =>split(reason_for_loss, ';'), OUTER => TRUE) reasons

)

SELECT *
FROM flatten