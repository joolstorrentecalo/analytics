
----All Subscriptions from Source that includes RampID and Legacy RampID
With dim_subscription_source AS (

  SELECT distinct
    sub.accountid                    AS dim_crm_account_id,
    sub.id                           AS subscription_id,
    sub.name                         AS subscription_name,
    sub.version                      AS subscription_version,
    sub.status                       AS subscription_status,
    sub.termstartdate                AS term_start_date,
    sub.termenddate                  AS term_end_date,
    rampid                           AS ramp_id, ---Identifies ramps from booked via current Ramp functionality
    CASE when sub.rampid <> '' OR sub.rampid IS NOT NULL THEN rampid
    ELSE 'Not a ramp' END            AS is_ramp,
    MULTIYEARDEALSUBSCRIPTIONLINKAGE__C AS myb_opportunity_id,---Equivalent to SSP ID in SF, deprecated now, used for identifying Legacy ramps
    sub.opportunityid__c                AS opportunity_id
  FROM RAW.zuora_stitch.subscription sub

---Legacy Zuora Ramps 
---Historical Ramp Deals for data >= Sep 2021
---myb_opportunity_id should have a value of SSP_ID
), zuora_legacy_ramps AS (

    SELECT 
      dim_crm_account_id,
      subscription_id,
      subscription_name,
      subscription_version,
      subscription_status,
      term_start_date,
      term_end_date,
      ramp_id,
      is_ramp,
      myb_opportunity_id,
      opportunity_id
    FROM dim_subscription_source
    WHERE 
      myb_opportunity_id != '' 
      AND myb_opportunity_id is not NULL 
      AND myb_opportunity_id!= 'Not a ramp' 
      AND (is_ramp = '' or is_ramp IS NULL)
      

---Current Ramps from Zuora Ramps Functionality
), zuora_ramps AS (

    SELECT 
      dim_crm_account_id,
      subscription_id,
      subscription_name,
      subscription_version,
      subscription_status,
      term_start_date,
      term_end_date,
      ramp_id,
      is_ramp,
      myb_opportunity_id,
      opportunity_id
    FROM dim_subscription_source
      WHERE ramp_id <> '' and ramp_id is not null 
      and ramp_id <> 'Not a ramp'


--- Legacy SF Ramps 
--- Historical Ramp Deals for data <= October 2021
), sheetload_map_ramp_deals AS (

  SELECT  * 
    FROM 
   PROD.RESTRICTED_SAFE_LEGACY.SHEETLOAD_MAP_RAMP_DEALS
    WHERE "Overwrite_SSP_ID" IS NOT NULL


--Identifying Ramp Deals from SF by using Opportunity_category
--Opportunity_category is manually updated, over 90% accurate
), ramp_deals AS (

   SELECT 
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.ssp_id, 
      dim_crm_opportunity.opportunity_term	
    FROM PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY mart_crm_opportunity		
    INNER JOIN PROD.RESTRICTED_SAFE_COMMON.DIM_CRM_OPPORTUNITY	dim_crm_opportunity			
      ON LEFT(dim_crm_opportunity.dim_crm_opportunity_id,15) = LEFT(mart_crm_opportunity.ssp_id,15)				
    WHERE ssp_id IS NOT NULL 
      AND mart_crm_opportunity.opportunity_category LIKE '%Ramp Deal%'


), ramp_deals_ssp_id_multiyear_linkage AS (

    SELECT 
      zuora_ramps.subscription_name,
      dim_crm_opportunity.dim_crm_opportunity_id, 
      CASE
       WHEN sheetload_map_ramp_deals.dim_crm_opportunity_id IS NOT NULL THEN sheetload_map_ramp_deals."Overwrite_SSP_ID" 
       WHEN zuora_legacy_ramps.opportunity_id IS NOT NULL THEN zuora_legacy_ramps.myb_opportunity_id
       WHEN zuora_ramps.opportunity_id IS NOT NULL THEN zuora_ramps.myb_opportunity_id
        WHEN ramp_deals.dim_crm_opportunity_id IS NOT NULL THEN ramp_deals.ssp_id     
      END AS ramp_ssp_id_init,
      CASE WHEN ramp_ssp_id_init <> 'Not a ramp' THEN ramp_ssp_id_init
      ELSE LEFT(zuora_ramps.opportunity_id, 15) END AS ramp_ssp_id,
      zuora_legacy_ramps.opportunity_id as zuora_legacy_opp_id,
      zuora_ramps.opportunity_id as zuora_opp_id,
      sheetload_map_ramp_deals.dim_crm_opportunity_id as sheetload_opp_id,
      ramp_deals.dim_crm_opportunity_id as sf_ramp_deal_opp_id

    FROM PROD.RESTRICTED_SAFE_COMMON.DIM_CRM_OPPORTUNITY	dim_crm_opportunity	        
    LEFT JOIN sheetload_map_ramp_deals        
     ON sheetload_map_ramp_deals.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id 
   LEFT JOIN ramp_deals          
     ON ramp_deals.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
   LEFT JOIN zuora_legacy_ramps
     ON zuora_legacy_ramps.opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN zuora_ramps
     ON zuora_ramps.opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    WHERE ramp_ssp_id IS NOT NULL 


--Getting Subscription information
), subscriptions_with_ssp_id AS (

    SELECT 
      ramp_deals_ssp_id_multiyear_linkage.ramp_ssp_id,
      ramp_deals_ssp_id_multiyear_linkage.dim_crm_opportunity_id as crm_opportunity_id,
      dim_subscription.*				
    FROM PROD.common.dim_subscription			
    LEFT JOIN ramp_deals_ssp_id_multiyear_linkage				
    ON dim_subscription.dim_crm_opportunity_id = ramp_deals_ssp_id_multiyear_linkage.dim_crm_opportunity_id	

    
--Getting Last term version of the subscription         
), dim_subscription_latest_version AS (

    SELECT 
      ROW_NUMBER() OVER (PARTITION BY subscription_name, term_end_date ORDER BY ramp_ssp_id, subscription_version DESC) AS last_term_version,
      subscriptions_with_ssp_id.*       
    FROM subscriptions_with_ssp_id        
    Where subscription_status != 'Cancelled'        
    QUALIFY last_term_version = 1   


), dim_subscription_cancelled AS (  

    SELECT DISTINCT 
      subscription_name, 
      term_start_date 
    FROM PROD.common.dim_subscription	     
    Where subscription_status = 'Cancelled' 


), dim_subscription_base AS (     

    SELECT 
      dim_subscription_latest_version.*
    FROM dim_subscription_latest_version        
    LEFT JOIN dim_subscription_cancelled        
      ON dim_subscription_latest_version.subscription_name = dim_subscription_cancelled.subscription_name       
      AND dim_subscription_latest_version.term_start_date = dim_subscription_cancelled.term_start_date        
    WHERE dim_subscription_cancelled.subscription_name IS NULL  


), ramp_min_max_dates AS (

    SELECT 
      ramp_ssp_id, 
      MIN(term_start_date) AS min_term_start_date,  
      MAX(term_end_date) AS max_term_end_date  
    FROM dim_subscription_base      
    WHERE ramp_ssp_id IS NOT NULL       
    GROUP BY 1 HAVING COUNT(*) > 1  


), subscriptions_for_ramp_deals AS (    

    SELECT 
      dim_subscription_base.*, 
      CASE WHEN min_term_start_date IS NOT NULL THEN min_term_start_date 
      ELSE term_start_date 
      END AS ATR_term_start_date,       
      CASE WHEN max_term_end_date IS NOT NULL THEN max_term_end_date 
      ELSE term_end_date END AS ATR_term_end_date       
    FROM dim_subscription_base        
    LEFT JOIN ramp_min_max_dates       
      ON dim_subscription_base.ramp_ssp_id = ramp_min_max_dates.ramp_ssp_id        
    WHERE dim_subscription_base.ramp_ssp_id IS NULL
     OR (dim_subscription_base.ramp_ssp_id IS NOT NULL 
     AND max_term_end_date != term_end_date)  
   

  --ARR from charges    
), subscription_charges AS (

    SELECT 
      subscriptions_for_ramp_deals.dim_subscription_id,
      fct_charge.dim_charge_id,
      dim_product_detail_id,
      subscriptions_for_ramp_deals.crm_opportunity_id as dim_crm_opportunity_id,
      fct_charge.dim_billing_account_id,
      dim_crm_user.crm_user_sales_segment,
      dim_crm_user.crm_user_geo,
      dim_crm_user.crm_user_region,
      dim_crm_user.crm_user_area,
      dim_crm_user.dim_crm_user_id,
      user_name,
      subscriptions_for_ramp_deals.ATR_term_start_date,
      subscriptions_for_ramp_deals.ATR_term_end_date,
      subscriptions_for_ramp_deals.dim_crm_account_id, 
      subscriptions_for_ramp_deals.subscription_name,
      quantity, 
      ARR     
    FROM subscriptions_for_ramp_deals     
    LEFT JOIN  PROD.RESTRICTED_SAFE_COMMON.FCT_CHARGE   fct_charge   
      ON subscriptions_for_ramp_deals.dim_subscription_id = fct_charge.dim_subscription_id        
      AND subscriptions_for_ramp_deals.term_end_date = TO_VARCHAR(TO_DATE(TO_CHAR(effective_end_date_id),'yyyymmdd'), 'YYYY-MM-DD')       
      AND fct_charge.effective_start_date_id != fct_charge.effective_end_date_id        
    LEFT JOIN PROD.RESTRICTED_SAFE_COMMON.DIM_CHARGE    dim_charge  
      ON dim_charge.dim_charge_id = fct_charge.dim_charge_id 
    INNER JOIN PROD.COMMON.dim_billing_account dim_billing_account
      ON fct_charge.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN PROD.RESTRICTED_SAFE_COMMON.dim_crm_account dim_crm_account
      ON dim_crm_account.dim_crm_account_id = dim_billing_account.dim_crm_account_id
    LEFT JOIN PROD.COMMON.dim_crm_user dim_crm_user
      ON dim_crm_account.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    WHERE fct_charge.dim_product_detail_id IS NOT NULL  
    AND dim_crm_account.is_jihu_account != 'TRUE'
    AND ARR != 0 AND is_included_in_arr_calc = 'TRUE'

    
--Final ATR 
), final AS ( 

    SELECT DISTINCT
      dim_date.fiscal_quarter_name_fy, 
      dim_crm_account_id, 
      dim_crm_opportunity_id,
      dim_subscription_id, 
      subscription_name,
      dim_billing_account_id,
      dim_charge_id,
      dim_product_detail_id,
      ATR_term_start_date,
      ATR_term_end_date,
      dim_crm_user_id,
      user_name,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      SUM(ARR) as ARR, 
      Quantity 
    FROM subscription_charges 
    LEFT JOIN PROD.COMMON.DIM_DATE  
     ON subscription_charges.ATR_term_end_date = dim_date.date_day 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18
)

{{ dbt_audit(
cte_ref="final",
created_by="@snalamaru",
updated_by="@snalamaru",
created_date="2024-04-01",
updated_date="2024-04-01"
) }}


