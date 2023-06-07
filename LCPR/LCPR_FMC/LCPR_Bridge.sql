--------------------------------------------------------------------------------
----------------- BRIDGE ORDER ACTIVITY & CUSTOMER SERVICE RATES ---------------
--------------------------------------------------------------------------------

WITH

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-05-01')) AS input_month
        ,85 as overdue_days
        , 'NON PAY' as dx_type, '2. Fixed Involuntary Churner' as fix_churn
        -- , 'V_DISCO' as dx_type, '1. Fixed Voluntary Churner' as fix_churn 
)

, fixedtable as (
SELECT
    fix_s_att_account, 
    -- fix_b_mes_overdue,
    -- fix_e_mes_overdue,
    -- fix_b_mes_numrgus, 
    -- fix_e_mes_numrgus, 
    fix_s_fla_mainmovement,
    fix_s_fla_churnflag,
    fix_s_fla_churntype
FROM "db_stage_dev"."lcpr_fixed_may2023"
)

, dna_bom as (
SELECT
    *
FROM(
    SELECT
        -- count(distinct sub_acct_no_sbb)
        *,
        row_number() over (partition by sub_acct_no_sbb order by dt desc) as nm_bom
    FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
    WHERE
        play_type <> '0P'
        and cust_typ_sbb = 'RES' 
        and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters) - interval '1' month
    )
WHERE nm_bom = 1
)


, dna_eom as (
SELECT
    *
FROM (
    SELECT
        -- count(distinct sub_acct_no_sbb)
        *,
        row_number() over (partition by sub_acct_no_sbb order by dt desc) as nm_eom
    FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
    WHERE
        play_type <> '0P'
        and cust_typ_sbb = 'RES' 
        and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
    )
WHERE nm_eom = 1
)


, order_activity as (
SELECT
    *
FROM (
SELECT
    *, 
    BEF_VIDEO + BEF_HSD + BEF_VOICE AS B_RGUS, 
    aft_video + aft_hsd + aft_voice as E_RGUS,
    row_number() over (partition by sub_acct_no_ooi order by create_dte_ocr desc) as r_nm
FROM "lcpr.sandbox.dev"."transactions_orderactivity"
WHERE 
    date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters)
    and acct_type = 'R'
    and ord_typ = (SELECT dx_type FROM parameters)
)
WHERE r_nm = 1 
    -- and ord_typ = (SELECT dx_type FROM parameters)
)

, disconnections_so as (
SELECT
    account_id, 
    DATE(completed_date) AS end_date,
    DATE(order_start_date) AS start_date, 
    case 
        when order_type = 'V_DISCO' then 'vol dx'
        when order_type = 'NON PAY' then 'invol dx'
        when trim(order_type) = 'RELOCATION/TRAN' or cease_reason_desc = 'MIG COAX TO FIB' then 'transfer'
        else 'otro'
    end as dx_type
FROM "lcpr.stage.prod"."so_hdr_lcpr"
WHERE
    date_trunc('month', date(completed_date)) = (SELECT input_month FROM parameters)
)

, full_joins as (
SELECT
    A.*, 
    B.sub_acct_no_sbb as dna_bom_id, 
    C.sub_acct_no_sbb as dna_eom_id,
    D.sub_acct_no_ooi as oa_id, 
    E.dx_type
FROM fixedtable A
FULL OUTER JOIN dna_bom B
    ON A.fix_s_att_account = B.sub_acct_no_sbb
FULL OUTER JOIN dna_eom C
    ON A.fix_s_att_account = C.sub_acct_no_sbb
FULL OUTER JOIN order_activity D
    ON A.fix_s_att_account = D.sub_acct_no_ooi
FULL OUTER JOIN disconnections_so E
    ON A.fix_s_att_account = E.account_id
)

SELECT * FROM full_joins

--- Misclassified but already in the table
-- SELECT
--     -- distinct fix_s_fla_mainmovement,
--     fix_s_fla_churntype,
--     -- count(distinct fix_s_att_account)
--     -- fix_b_att_active, 
--     -- fix_e_att_active, 
--     -- fix_b_mes_numrgus, 
--     -- fix_e_mes_numrgus, 
--     count(distinct fix_s_att_account) as Accounts
-- FROM "db_stage_dev"."lcpr_fixed_may2023"
-- WHERE
--     fix_s_att_account in (SELECT sub_acct_no_ooi FROM order_activity)
--     and fix_s_fla_mainmovement = '6.Null last day'
-- GROUP BY 1
-- ORDER BY 1

-- , examples as (
-- SELECT
--     fix_s_fla_mainmovement, 
--     sub_acct_no_ooi,
--     -- B.order_type,
--     -- (b_rgus - e_rgus) as rgus_diff,
--     b_rgus, 
--     e_rgus, 
--     order_type
--     -- count(distinct sub_acct_no_ooi) as Accounts
-- FROM "db_stage_dev"."lcpr_fixed_may2023" A
-- LEFT JOIN (
--     SELECT
--     *
--     FROM (
--     SELECT
--         *, 
--         BEF_VIDEO + BEF_HSD + BEF_VOICE AS B_RGUS, 
--         aft_video + aft_hsd + aft_voice as E_RGUS,
--         ord_typ as order_type,
--         row_number() over (partition by sub_acct_no_ooi order by create_dte_ocr desc) as r_nm
--     FROM "lcpr.sandbox.dev"."transactions_orderactivity"
--     WHERE 
--         date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters)
--         and acct_type = 'R'
--         )
--     WHERE r_nm = 1 
--         -- and order_type = (SELECT dx_type FROM parameters)
--     ) B
--     ON A.fix_s_att_account = B.sub_acct_no_ooi
-- WHERE fix_s_att_account in (SELECT sub_acct_no_ooi FROM order_activity)
-- -- GROUP BY 1, 2
-- ORDER BY 1, 2
-- )

-- SELECT
--     *
-- FROM examples
-- WHERE sub_acct_no_ooi is not null


-- SELECT
--     -- *
--     ls_chg_dte_ocr, 
--     order_no_ooi, 
--     sub_acct_no_ooi, 
--     ord_typ, 
--     BEF_VIDEO + BEF_HSD + BEF_VOICE AS B_RGUS, 
--     aft_video + aft_hsd + aft_voice as E_RGUS
-- FROM "lcpr.sandbox.dev"."transactions_orderactivity"
-- WHERE
--     date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters)
    -- and sub_acct_no_ooi = 8211080500139464
    -- and sub_acct_no_ooi = 8211080500143771

-- SELECT
--     -- distinct fix_s_fla_mainmovement,
--     -- fix_s_fla_churntype,
--     -- count(distinct fix_s_att_account)
--     fix_b_att_active, 
--     fix_e_att_active, 
--     fix_b_mes_numrgus, 
--     fix_e_mes_numrgus, 
--     count(distinct fix_s_att_account)
-- FROM "db_stage_dev"."lcpr_fixed_may2023"
-- WHERE
--     fix_s_att_account not in (SELECT sub_acct_no_ooi FROM order_activity)
--     and fix_s_fla_churntype = (SELECT fix_churn FROM parameters)
-- GROUP BY 1, 2, 3, 4

-- SELECT
--     fix_s_fla_churntype, count(distinct fix_s_att_account)
-- FROM "db_stage_dev"."lcpr_fixed_may2023"
-- WHERE 
--     fix_s_att_account in (SELECT fix_s_att_account FROM (SELECT *, row_number() over (partition by fix_s_att_account) as r_n FROM "db_stage_dev"."lcpr_fixed_may2023") WHERE r_n > 1)
--     and fix_s_att_account in (SELECT sub_acct_no_ooi FROM order_activity)
-- GROUP BY 1
-- ORDER BY 1, 2

-- SELECT
--     fix_s_fla_churntype, 
--     dx_type, 
--     count(distinct fix_s_att_account)
-- FROM full_joins
-- WHERE fix_s_att_account in (SELECT sub_acct_no_ooi FROM order_activity)
-- GROUP BY 1, 2
-- ORDER BY 1, 2, 3

-- SELECT
--     count(distinct case when fix_s_att_account is not null and oa_id is not null then oa_id else null end) as match, 
--     count(distinct oa_id) as all_oa_customers
-- FROM full_joins

-- SELECT
--     -- *
--     case 
--         when delinquency_days > 85 then 'overdue > 85'
--         when delinquency_days <= 85 then 'overdue <= 85'
--         when delinquency_days is null then 'null overdue'
--     else null end as overdue_class, 
--     count(distinct sub_acct_no_sbb)
-- FROM dna_bom
-- WHERE sub_acct_no_sbb not in (SELECT fix_s_att_account FROM fixedtable)
--     and sub_acct_no_sbb in (SELECT sub_acct_no_ooi FROM order_activity)
-- GROUP BY 1


-- SELECT
--     b_rgus, 
--     e_rgus, 
--     count(distinct sub_acct_no_ooi)
-- FROM order_activity
-- WHERE 
--     sub_acct_no_ooi not in (SELECT sub_acct_no_sbb FROM dna_bom)
--     and sub_acct_no_ooi not in (SELECT fix_s_att_account FROM fixedtable)
-- GROUP BY 1, 2
-- ORDER BY 1, 2

-- SELECT
--     count(distinct fix_s_att_account)
-- FROM fixedtable
-- WHERE
--     fix_s_fla_churntype = (SELECT fix_churn FROM parameters)
--     and fix_s_att_account not in (SELECT sub_acct_no_ooi FROM order_activity)

-- SELECT
--     count(distinct sub_acct_no_ooi)
-- FROM order_activity
-- WHERE
--     sub_acct_no_ooi not in (SELECT sub_acct_no_sbb FROM dna_bom)
--     and sub_acct_no_ooi not in (SELECT fix_s_att_account FROM fixedtable)

-- SELECT
--     -- vol_disco_rsn, , 
--     -- B_RGUS, 
--     -- E_RGUS,
--     -- count(distinct sub_acct_no_ooi)
--     *
-- FROM order_activity
-- WHERE sub_acct_no_ooi in (SELECT oa_id FROM missing)
-- GROUP BY 1, 2

-- SELECT
--     -- fix_s_fla_mainmovement, 
--     fix_s_fla_churntype,
--     count(distinct oa_id)
-- FROM full_joins
-- GROUP BY 1
