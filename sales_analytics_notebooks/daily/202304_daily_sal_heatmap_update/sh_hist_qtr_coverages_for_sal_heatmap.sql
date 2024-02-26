SELECT
    key_agg_day,
    agg_key_name,
    agg_key_value,
    close_day_of_fiscal_quarter_normalised AS agg_key_day,
    bookings_linearity,
    open_1plus_net_arr_coverage,
    open_3plus_net_arr_coverage,
    open_4plus_net_arr_coverage,
    rq_plus_1_open_1plus_net_arr_coverage,
    rq_plus_1_open_3plus_net_arr_coverage,
    rq_plus_1_open_4plus_net_arr_coverage,
    rq_plus_2_open_1plus_net_arr_coverage,
    rq_plus_2_open_3plus_net_arr_coverage,
    rq_plus_2_open_4plus_net_arr_coverage
FROM prod.workspace_sales.rsa_source_coverage_qtr_fitted_curves
WHERE close_day_of_fiscal_quarter_normalised > (
    SELECT DISTINCT day_of_fiscal_quarter_normalised
    FROM prod.workspace_sales.date_details
    WHERE date_actual = CURRENT_DATE
) - 2
AND close_day_of_fiscal_quarter_normalised < (
    SELECT DISTINCT day_of_fiscal_quarter_normalised
    FROM prod.workspace_sales.date_details
    WHERE date_actual = CURRENT_DATE
) + 5
AND agg_key_name IN ('key_bu', 'key_bu_subbu', 'key_bu_subbu_division')
