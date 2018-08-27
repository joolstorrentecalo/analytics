with base as ( 

	SELECT *
	FROM {{ref('historical_regional_quota')}}

), emea as (

	SELECT month_of,
			'emea'::varchar as team, 
			emea as quota
	FROM base

), public_sector as (

	SELECT month_of,
			'public_sector'::varchar as team, 
			public_sector as quota
	FROM base

), us_east as (

	SELECT month_of,
			'us_east'::varchar as team, 
			us_east as quota
	FROM base

), us_central as (

	SELECT month_of,
			'us_central'::varchar as team, 
			us_central as quota
	FROM base

), us_west as (

	SELECT month_of,
			'us_west'::varchar as team, 
			us_west as quota
	FROM base

), apac as (

	SELECT month_of,
			'apac'::varchar as team, 
			apac as quota
	FROM base

), channel as (

	SELECT month_of,
			'channel'::varchar as team, 
			channel as quota
	FROM base

), self_serve as (

	SELECT month_of,
			'self_serve_smb'::varchar as team, 
			self_serve as quota
	FROM base

), unioned as (

SELECT * FROM emea
UNION ALL
SELECT * FROM public_sector
UNION ALL
SELECT * FROM us_east
UNION ALL
SELECT * FROM us_central
UNION ALL
SELECT * FROM us_west
UNION ALL
SELECT * FROM apac
UNION ALL
SELECT * FROM channel
UNION ALL
SELECT * FROM self_serve

)

SELECT md5(month_of||team) as region_quota_id,
		month_of as quota_month,
  		team as region,
  		quota
FROM unioned
