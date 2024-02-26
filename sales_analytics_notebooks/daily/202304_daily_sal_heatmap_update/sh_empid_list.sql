WITH sfdc_users_xf AS (

    SELECT *
    FROM prod.workspace_sales.sfdc_users_xf
    WHERE
        key_sal_heatmap NOT LIKE '%other%'
        AND lower(asm
        ) NOT LIKE '%all%'
        AND is_active = 1
        AND is_rep_flag = 1

)

SELECT
    'key_bu' AS key_name,
    key_bu   AS key_value,
    key_sal_heatmap
FROM sfdc_users_xf
GROUP BY 1, 2, 3
UNION
SELECT
    'key_bu_subbu' AS key_name,
    key_bu_subbu   AS key_value,
    key_sal_heatmap
FROM sfdc_users_xf
GROUP BY 1, 2, 3
UNION
SELECT
    'key_bu_subbu_division' AS key_name,
    key_bu_subbu_division   AS key_value,
    key_sal_heatmap
FROM sfdc_users_xf
GROUP BY 1, 2, 3
