{% snapshot job_profiles_snapshots %}

    {{
        config(
          unique_key='job_code',
          strategy='check',
          check_cols=[
            'job_code',
            'job_profile',
            'inactive',
            'job_family',
            'management_level',
            'job_level',
            '_fivetran_deleted']
        )
    }}
    
    SELECT * 
    FROM {{ source('workday','job_profiles') }}
    
{% endsnapshot %}