WITH

parameters as (SELECT date('2023-04-01'))

-- , intramonth_churn as (
-- SELECT
--     sub_acct_no_ooi as account_id
-- FROM (
--     SELECT
--         sub_acct_no_ooi, 
--         first_value(ord_typ) over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr asc) as first_order_type,
--         first_value(ord_typ) over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr desc) as last_order_type
--     FROM "db_stage_dev"."orderactivity_mar_apr_2023"
--     WHERE  
--         (
--         (ord_typ = 'V_DISCO' and order_rsn != 'VL') 
--         or 
--         (ord_typ = 'NON PAY' and order_rsn = 'NP'
--         --- Subsidized customers (consider just for Invol Churn)
--         and sub_acct_no_ooi not in 
--             (
--          SELECT 
--                 sub_acct_no_sbb 
--             FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
--             WHERE
--                 play_type <> '0P'
--                 and cust_typ_sbb = 'RES' 
--                 and date(dt) = date('2023-04-01')
--                 and acp is not null
--             )
--         )
--         )
--     )
-- WHERE
--     last_order_type not in ('V_DISCO', 'NON PAY')
-- )

, intramonth_churn as (
SELECT
    sub_acct_no_ooi as account_id
FROM "db_stage_dev"."orderactivity_mar_apr_2023"
WHERE
    ord_typ in ('V_DISCO', 'NON PAY')
    and sub_acct_no_ooi in (SELECT sub_acct_no_ooi FROM "db_stage_dev"."orderactivity_mar_apr_2023" WHERE ord_typ = 'RESTART')
)

SELECT 
    -- distinct disco_rsn_sbb,
    count(distinct sub_acct_no_ooi) as accounts, 
    sum(b_rgus) as rgus
    -- *
FROM (
SELECT
    *, 
    BEF_VIDEO + BEF_HSD + BEF_VOICE AS B_RGUS, 
    aft_video + aft_hsd + aft_voice as E_RGUS,
    row_number() over (partition by sub_acct_no_ooi order by create_dte_ocr desc) as r_nm
FROM "db_stage_dev"."orderactivity_mar_apr_2023"
WHERE 
    date_trunc('month', date(ls_chg_dte_ocr)) = date('2023-04-01')
    and acct_type = 'R'
    -- and ord_typ = 'V_DISCO' and order_rsn != 'VL' --- Exclude transfers
    -- and ord_typ = 'NON PAY' and order_rsn = 'NP'
    -- and ord_typ in ('V_DISCO', 'NON PAY')
    and (
        (ord_typ = 'V_DISCO' and order_rsn != 'VL') 
        or 
        (ord_typ = 'NON PAY' and order_rsn = 'NP'
        --- Subsidized customers (consider just for Invol Churn)
        and sub_acct_no_ooi not in 
            (
         SELECT 
                sub_acct_no_sbb 
            FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
            WHERE
                play_type <> '0P'
                and cust_typ_sbb = 'RES' 
                and date(dt) = date('2023-04-01')
                and acp is not null
            )
        )
        )
    and sub_acct_no_ooi not in (SELECT account_id FROM intramonth_churn)
)
WHERE r_nm = 1 
    -- and ord_typ = (SELECT dx_type FROM parameters)
-- GROUP BY 1



