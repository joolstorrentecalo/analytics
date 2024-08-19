{{config({
    "materialized": "table",
    "transient": false
  })
}}

{{ simple_cte([
    ('sheetload_map_ramp_deals','sheetload_map_ramp_deals'),
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('dim_crm_opportunity','dim_crm_opportunity'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_charge', 'dim_charge'),
    ('fct_charge','fct_charge'),
    ('dim_billing_account','dim_billing_account'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user', 'dim_crm_user'),
    ('dim_date', 'dim_date')

]) }}

----All Subscriptions from Source that include RampID and Legacy RampID
, prep_subscription AS (

  SELECT distinct
    dim_crm_account_id,
    dim_subscription_id,
    subscription_name,
    subscription_version,
    subscription_status,
    term_start_date,
    term_end_date,
    ramp_id, ---Identifies ramps booked via current Ramp functionality
    multi_year_deal_subscription_linkage AS myb_opportunity_id,---Equivalent to SSP ID in SF, deprecated now, used for identifying Legacy ramps,only the first year of ramp can ve identified by this opp_id
    dim_opportunity_id,
    CASE 
      WHEN ramp_id != '' OR  (multi_year_deal_subscription_linkage != '' AND multi_year_deal_subscription_linkage IS NOT NULL AND multi_year_deal_subscription_linkage != 'Not a ramp') 
        THEN 'Yes' 
        ELSE 'No' 
    END AS is_ramp
  FROM {{ ref('prep_subscription') }} 


---Legacy Zuora Ramps - historical Ramp Deals for data >= Sep 2021
---myb_opportunity_id should have a value of SSP_ID
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
    FROM prep_subscription
    WHERE 
      is_ramp = 'Yes'
      

--- Legacy SF Ramps - historical Ramp Deals for data <= October 2021
), sheetload_map_ramp_deal AS (

  SELECT  * 
  FROM 
   sheetload_map_ramp_deals
  WHERE "Overwrite_SSP_ID" IS NOT NULL


--Identifying Ramp Deals from SF by using Opportunity_category
--Opportunity_category is manually updated in SF, over 90% accuracy rate
), ramp_deals AS (

   SELECT 
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.ssp_id, 
      dim_crm_opportunity.opportunity_term	
    FROM mart_crm_opportunity		
    INNER JOIN dim_crm_opportunity			
     ON LEFT(dim_crm_opportunity.dim_crm_opportunity_id,15) = LEFT(mart_crm_opportunity.ssp_id,15)				
    WHERE ssp_id IS NOT NULL 
     AND mart_crm_opportunity.opportunity_category LIKE '%Ramp Deal%'


---Combining All Ramp deals from SF and Zuora sources
), ramp_deals_ssp_id_multiyear_linkage AS (

    SELECT 
      zuora_ramps.subscription_name,
      dim_crm_opportunity.dim_crm_opportunity_id, 
      CASE
        WHEN sheetload_map_ramp_deal.dim_crm_opportunity_id IS NOT NULL THEN sheetload_map_ramp_deal."Overwrite_SSP_ID" 
        WHEN zuora_ramps.opportunity_id IS NOT NULL THEN zuora_ramps.myb_opportunity_id
        WHEN ramp_deals.dim_crm_opportunity_id IS NOT NULL THEN ramp_deals.ssp_id     
      END AS ramp_ssp_id_init,
      CASE 
        WHEN ramp_ssp_id_init != 'Not a ramp' 
          THEN ramp_ssp_id_init
        ELSE LEFT(zuora_ramps.opportunity_id, 15) 
      END AS ramp_ssp_id,
      zuora_ramps.opportunity_id                     AS zuora_opp_id,
      sheetload_map_ramp_deal.dim_crm_opportunity_id AS sheetload_opp_id,
      ramp_deals.dim_crm_opportunity_id              AS sf_ramp_deal_opp_id
    FROM dim_crm_opportunity	        
    LEFT JOIN sheetload_map_ramp_deal       
     ON sheetload_map_ramp_deal.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id 
    LEFT JOIN ramp_deals          
     ON ramp_deals.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN zuora_ramps
     ON zuora_ramps.opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    WHERE ramp_ssp_id IS NOT NULL 

--Getting Subscription information
), subscriptions_with_ssp_id AS (

    SELECT 
      ramp_deals_ssp_id_multiyear_linkage.ramp_ssp_id,
      dim_subscription.*,
    CASE WHEN ramp_ssp_id IS NULL AND LEAD(term_start_month) OVER (PARTITION BY dim_subscription.subscription_name ORDER BY ramp_ssp_id,subscription_version) = term_start_month THEN TRUE					 
         WHEN RAMP_SSP_ID IS NOT NULL THEN FALSE														
         ELSE FALSE
    END AS is_dup_term				
    FROM dim_subscription			
    LEFT JOIN ramp_deals_ssp_id_multiyear_linkage				
     ON dim_subscription.dim_crm_opportunity_id = ramp_deals_ssp_id_multiyear_linkage.dim_crm_opportunity_id
    WHERE
    --data quality, last version is expired with no ARR in mart_arr. Should filter it out completely.
      dim_subscription_id NOT IN ('2c92a0ff5e1dcf14015e3bb595f14eef','2c92a0ff5e1dcf14015e3c191d4f7689','2c92a007644967bc01645d54e7df49a8', '2c92a007644967bc01645d54e9b54a4b', '2c92a0ff5e1dcf1a015e3bf7a32475a5')
      --test subscription
      AND dim_subscription.subscription_name != 'Test- New Subscription'
      --data quality, last term not entered with same pattern, sub_name = A-S00022101
      AND dim_subscription_id != '2c92a00f7579c362017588a2de19174a'
      --term dates do not align to the subscription term dates, sub_name = A-S00038937
      AND dim_subscription_id != '2c92a01177472c5201774af57f834a43'
      --data quality, last term not entered with same pattern that fits ATR logic. Edge cases that needs to be filtered out to get to the last term version that should count for this subscription.
      --sub_name = A-S00011774
      AND dim_subscription_id NOT IN ('8a1298657dd7f81d017dde1bd9c03fa8','8a128b317dd7e89a017ddd38a74d3037','8a128b317dd7e89a017ddd38a6052ff0',
                                      '8a128b317dc30baa017dc41e5b0932e9','8a128b317dc30baa017dc41e59dd32be','8a128b317dc30baa017dc41e58b43295',
                                      '2c92a0fd7cc1ab13017cc843195f62fb','2c92a0fd7cc1ab13017cc843186f62da','2c92a0fd7cc1ab13017cc843178162b6',
                                      '2c92a0fd7cc1ab13017cc843164d6292')


---Handling Outliers from subscriptions data
), dim_subscription_int AS (

    SELECT
      subscriptions_with_ssp_id.*,
      CASE
        WHEN LEAD(term_end_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            = term_end_month THEN TRUE
        WHEN LEAD(term_end_month,2) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            = term_end_month THEN TRUE
        WHEN LEAD(subscription_end_fiscal_year) OVER (PARTITION BY subscription_name ORDER BY
            subscription_version) = subscription_end_fiscal_year THEN TRUE
        WHEN LEAD(term_start_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            = term_start_month THEN TRUE
        --check for subsequent subscriptiptions that are backed out
        WHEN LEAD(term_start_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,2) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,3) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,4) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,5) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,6) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,7) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,8) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,9) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,10) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,11) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,12) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,13) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,14) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        ELSE FALSE
      END AS exclude_from_term_sorting
    FROM subscriptions_with_ssp_id
    WHERE is_dup_term = FALSE


--Getting Last term version of the subscription         
), subscriptions_last_term_version AS (

    SELECT 
      ROW_NUMBER() OVER (PARTITION BY subscription_name, term_end_date ORDER BY ramp_ssp_id, subscription_version DESC) AS last_term_version,
      dim_subscription_int.*             
    FROM dim_subscription_int
    WHERE exclude_from_term_sorting = FALSE


---Subscriptions base
), dim_subscription_base AS (

    SELECT
      dim_subscription.*,
      subscriptions_last_term_version.last_term_version,
      subscriptions_last_term_version.ramp_ssp_id
    FROM dim_subscription
    INNER JOIN subscriptions_last_term_version
      ON dim_subscription.dim_subscription_id = subscriptions_last_term_version.dim_subscription_id
    WHERE last_term_version = 1


--Calculating min and max term dates for all ramps inclusive of 1 year ramp deals
), ramp_min_max_dates AS (

    SELECT 
      ramp_ssp_id, 
      MIN(term_start_date) AS min_term_start_date,  
      MAX(term_end_date) AS max_term_end_date  
    FROM dim_subscription_base      
    WHERE ramp_ssp_id IS NOT NULL       
    GROUP BY 1 HAVING COUNT(*) >= 1  


----Calculating ATR start term and End term dates from Subscripion base
), subscriptions_for_all AS (    

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
     AND max_term_end_date = term_end_date) 


--ARR from charges and other columns as needed  
), subscription_charges AS (

    SELECT 
      subscriptions_for_all.dim_subscription_id,
      fct_charge.dim_charge_id,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name,
      dim_product_detail_id,
      subscriptions_for_all.dim_crm_opportunity_id,
      fct_charge.dim_billing_account_id,
      dim_crm_user.crm_user_sales_segment,
      dim_crm_user.crm_user_geo,
      dim_crm_user.crm_user_region,
      dim_crm_user.crm_user_area,
      dim_crm_user.dim_crm_user_id,
      dim_crm_user.user_name,
      subscriptions_for_all.ATR_term_start_date,
      subscriptions_for_all.ATR_term_end_date,
      subscriptions_for_all.dim_crm_account_id, 
      subscriptions_for_all.subscription_name,
      quantity, 
      ARR,
      zuora_renewal_subscription_name AS renewal_subscription_name 
    FROM subscriptions_for_all    
    LEFT JOIN fct_charge   
      ON subscriptions_for_all.dim_subscription_id = fct_charge.dim_subscription_id        
      AND subscriptions_for_all.term_end_date = TO_VARCHAR(TO_DATE(TO_CHAR(effective_end_date_id),'yyyymmdd'), 'YYYY-MM-DD')   
      AND fct_charge.effective_start_date_id != fct_charge.effective_end_date_id            
    INNER JOIN dim_billing_account
      ON fct_charge.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_charge  
      ON dim_charge.dim_charge_id = fct_charge.dim_charge_id 
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = dim_billing_account.dim_crm_account_id
    LEFT JOIN dim_crm_user
      ON dim_crm_account.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    WHERE fct_charge.dim_product_detail_id IS NOT NULL  
      AND dim_crm_account.is_jihu_account != 'TRUE'
      AND dim_charge.is_included_in_arr_calc = 'TRUE'


--Final ATR Calculation for all Quarters 
), final AS ( 

    SELECT DISTINCT

      --Primary Key
      {{ dbt_utils.generate_surrogate_key(['subscription_charges.dim_charge_id' ]) }} AS primary_key,
      subscription_charges.dim_charge_id                                              AS dim_charge_id,

      --Date dimensions  
      dim_date.fiscal_quarter_name_fy                 AS fiscal_quarter_name_fy, 
      dim_date.fiscal_year                            As fiscal_year,

      --Foreign Keys
      subscription_charges.dim_crm_account_id         AS dim_crm_account_id, 
      subscription_charges.dim_crm_opportunity_id     AS dim_crm_opportunity_id,
      subscription_charges.dim_subscription_id        AS dim_subscription_id, 
      subscription_charges.subscription_name          AS subscription_name,
      subscription_charges.dim_billing_account_id     AS dim_billing_account_id,
      subscription_charges.dim_product_detail_id      AS dim_product_detail_id,
      subscription_charges.dim_parent_crm_account_id  AS dim_parent_crm_account_id,
      subscription_charges.dim_crm_user_id            AS dim_crm_user_id,

      --Other attributes
      subscription_charges.renewal_subscription_name  AS renewal_subscription_name,
      subscription_charges.parent_crm_account_name    AS parent_crm_account_name,

      --User info
      subscription_charges.user_name                  AS user_name,

      --User geo-related attributes
      subscription_charges.crm_user_sales_segment     AS crm_user_sales_segment,
      subscription_charges.crm_user_geo               AS crm_user_geo,
      subscription_charges.crm_user_region            AS crm_user_region,
      subscription_charges.crm_user_area              AS crm_user_area,
      
      --ATR-related attributes including Seat Quantity & ARR 
      subscription_charges.ATR_term_start_date        AS ATR_term_start_date,
      subscription_charges.ATR_term_end_date          AS ATR_term_end_date,
      subscription_charges.quantity                   AS quantity,
      SUM(subscription_charges.ARR)                   AS ARR

    FROM subscription_charges 
    LEFT JOIN dim_date
     ON subscription_charges.ATR_term_end_date = dim_date.date_day 
    {{ dbt_utils.group_by(n=22) }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2024-08-19",
    updated_date="2024-08-19"
) }}
