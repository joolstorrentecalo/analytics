{% set repeated_column_metrics = 
    "COUNT(CASE WHEN capture_month = 'application_month' THEN application_id ELSE NULL END) AS total_candidates_applied,
    SUM(CASE WHEN capture_month = 'application_month' THEN accepted_offer ELSE NULL END)    AS total_offers_based_on_application_month,
    SUM(CASE WHEN capture_month='offer_sent_month' THEN 1 ELSE NULL END)                    AS total_sent_offers,
    SUM(CASE WHEN capture_month='offer_sent_month' THEN accepted_offer ELSE NULL END)       AS offers_accepted_based_on_sent_month,
    SUM(CASE WHEN capture_month = 'accepted_month' THEN accepted_offer ELSE NULL END)       AS offers_accepted,
    AVG(CASE WHEN capture_month = 'accepted_month' THEN time_to_offer ELSE NULL END)        AS time_to_offer_average, 
    MEDIAN(CASE WHEN capture_month = 'accepted_month' THEN time_to_offer ELSE NULL END)     AS time_to_offer_median
    " %}

{% set repeated_column_names_ratio_to_report =
    "(partition by month_date, breakout_type, department_name, division, eeoc_field_name order by month_date) " %}

WITH greenhouse_diversity_intermediate AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_diversity_intermediate') }}

), breakout AS (

    SELECT
      month_date,
      'kpi_level_breakout'                        AS breakout_type,
      null                                        AS department_name,
      null                                        AS division,
      null                                        AS eeoc_field_name,
      null                                        AS eeoc_values,
      {{repeated_column_metrics}}  
    FROM  greenhouse_diversity_intermediate
    WHERE LOWER(eeoc_field_name) = 'candidate_status'
    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT
      month_date,
      'all_attributes_breakout'                  AS breakout_type,
      department_name,
      division,
      eeoc_field_name,
      eeoc_values,
      {{repeated_column_metrics}}
    FROM  greenhouse_diversity_intermediate
    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT
      month_date,
      'department_division_breakout'                         AS breakout_type,
      department_name,
      division,
      null                                                     AS eeoc_field_name,
      null                                                     AS eeoc_values,
      {{repeated_column_metrics}}
    FROM  greenhouse_diversity_intermediate
    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT
      month_date,
      'division_breakout'                     AS breakout_type,
      null                                    AS department_name,
      division,
      eeoc_field_name,
      eeoc_values,
      {{repeated_column_metrics}}
    FROM  greenhouse_diversity_intermediate
    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT
      month_date,
      'eeoc_only_breakout'                    AS breakout_type,
      'NA'                                    AS department_name,
      'NA'                                    AS division,
      eeoc_field_name,
      eeoc_values,
      {{repeated_column_metrics}}
    FROM  greenhouse_diversity_intermediate
    group by 1,2,3,4,5,6

), aggregated AS (

    SELECT 
      breakout.*,
      CASE WHEN total_candidates_applied = 0 THEN NULL
           WHEN total_candidates_applied IS NULL THEN NULL
           ELSE total_offers_based_on_application_month/total_candidates_applied END  AS application_to_offer_percent,
      CASE WHEN total_sent_offers = 0 THEN NULL
           WHEN total_sent_offers IS NULL THEN NULL
           ELSE offers_accepted_based_on_sent_month/total_sent_offers END             AS offer_acceptance_rate_based_on_offer_month,
      IFF(total_candidates_applied = 0 , null,
          RATIO_TO_REPORT(total_candidates_applied) 
          OVER {{repeated_column_names_ratio_to_report}})                             AS percent_of_applicants,
      IFF(total_sent_offers = 0, null,      
          RATIO_TO_REPORT(total_sent_offers) 
          OVER {{repeated_column_names_ratio_to_report}})                             AS percent_of_offers_sent,
      IFF(offers_accepted=0, null,
          RATIO_TO_REPORT(offers_accepted) 
          OVER {{repeated_column_names_ratio_to_report}})                             AS percent_of_offers_accepted,
      MIN(total_candidates_applied) OVER {{repeated_column_names_ratio_to_report}}    AS min_applicants_breakout, 
      MIN(total_sent_offers) OVER {{repeated_column_names_ratio_to_report}}           AS min_sent_offers_for_breakout,
      MIN(offers_accepted) OVER {{repeated_column_names_ratio_to_report}}             AS min_total_offers_accepted_for_breakout
    FROM breakout
    
), final AS (

    SELECT 
      month_date,
      breakout_type,
      department_name,
      division,
      LOWER(eeoc_field_name)                                                            AS eeoc_field_name,
      eeoc_values,
      ----Applicant Level----

      IFF(min_applicants_breakout < 3, NULL, total_candidates_applied)                  AS total_candidates_applied,
      IFF(breakout_type IN ('kpi_level','eeoc_only_breakout'), 
            application_to_offer_percent, NULL)                                         AS application_to_offer_percent,
      IFF(min_applicants_breakout < 3, NULL, percent_of_applicants)                     AS percent_of_applicants,

      ---Offers Sent Level ---

      IFF(min_sent_offers_for_breakout < 3, NULL, total_sent_offers)                      AS total_sent_offers,
      IFF(min_sent_offers_for_breakout < 3, NULL, percent_of_offers_sent)                 AS percent_of_offers_sent,
      min_sent_offers_for_breakout,

      --Offers Accepted Level ---
      IFF(min_total_offers_accepted_for_breakout < 3, NULL, offers_accepted)              AS offers_accepted,
      IFF(min_total_offers_accepted_for_breakout < 3, NULL, percent_of_offers_accepted)   AS percent_of_offers_accepted,
      IFF(min_total_offers_accepted_for_breakout < 3, NULL, time_to_offer_average)        AS time_to_offer_average,
      IFF(min_total_offers_accepted_for_breakout < 3, NULL, time_to_offer_median)         AS time_to_offer_median,
      IFF(min_total_offers_accepted_for_breakout < 3, NULL, 
          offer_acceptance_rate_based_on_offer_month)                                     AS offer_acceptance_rate_based_on_offer_month 
    FROM aggregated
    WHERE month_date <= DATEADD(MONTH,-1,current_date())

)

SELECT * 
FROM final
