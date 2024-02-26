SELECT
    agg_key_value_day,
    agg_key_value,
    close_day_of_fiscal_year_normalised,
    cfy_open_1plus_net_arr_coverage,
    cfy_open_3plus_net_arr_coverage,
    n4q_open_1plus_net_arr_coverage,
    n4q_open_3plus_net_arr_coverage,
    cfy_pipe_generation_linearity_rate,
    cfy_booked_linearity_rate,
    last_updated_at
FROM prod.workspace_sales.rsa_source_coverage_fy_n4q_fitted_curves
WHERE
    (close_day_of_fiscal_year_normalised > (
        SELECT DISTINCT day_of_fiscal_year_normalised
        FROM prod.workspace_sales.date_details
        WHERE date_actual = CURRENT_DATE
    ) - 2
    AND close_day_of_fiscal_year_normalised < (
        SELECT DISTINCT day_of_fiscal_year_normalised
        FROM prod.workspace_sales.date_details
        WHERE date_actual = CURRENT_DATE
    ) + 5)
    OR
    (close_day_of_fiscal_year_normalised > (
        SELECT DISTINCT day_of_fiscal_quarter_normalised
        FROM prod.workspace_sales.date_details
        WHERE date_actual = CURRENT_DATE
    ) - 2
    AND close_day_of_fiscal_year_normalised < (
        SELECT DISTINCT day_of_fiscal_quarter_normalised
        FROM prod.workspace_sales.date_details
        WHERE date_actual = CURRENT_DATE
    ) + 5)
