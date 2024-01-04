WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'permission_set_assignment') }}

), renamed AS (

       SELECT
      
         --keys
         id AS permission_set_assignment_id,

         --info
         assigneeid AS assignee_id,
         permissionsetgroupid AS permission_set_group_id,
         permissionsetid AS permission_set_id,
         isactive AS is_active,

         --metadata
         systemmodstamp AS system_mod_stamp,
         _sdc_batched_at AS sfdc_batched_at,
         _sdc_extracted_at AS sfdc_extracted_at,
         _sdc_received_at AS sfdc_received_at,
         _sdc_sequence AS sfdc_sequence,
         _sdc_table_version AS sfdc_table_version
      
       FROM source
)


SELECT *
FROM renamed

