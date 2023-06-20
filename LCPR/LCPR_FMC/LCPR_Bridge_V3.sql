WITH

parameters as (SELECT date('2023-04-01') as input_month)

-- , dna_bom as (
-- SELECT
--     *
-- FROM(
--     SELECT
--         -- count(distinct sub_acct_no_sbb)
--         *,
--         row_number() over (partition by sub_acct_no_sbb order by dt desc) as nm_bom
--     FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
--     WHERE
--         play_type <> '0P'
--         and cust_typ_sbb = 'RES' 
--         and date(dt) = (SELECT input_month FROM parameters)
--     )
-- WHERE nm_bom = 1
-- )

, dna_bom as (
SELECT
    sub_acct_no_sbb, 
    video, 
    hsd, 
    voice, 
    delinquency_days, 
    date(ls_pay_dte_sbb) as ls_pay_dte_sbb, 
    dt
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and date(dt) = (SELECT input_month FROM parameters)
)

, dna_eom as (
SELECT
    sub_acct_no_sbb, 
    video, 
    hsd, 
    voice, 
    delinquency_days, 
    date(ls_pay_dte_sbb) as ls_pay_dte_sbb,
    dt
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and date(dt) = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day
)

, dna_summary as (
SELECT
    -- distinct sub_acct_no_sbb as account_id, 
    -- first_value(dt) over (partition by sub_acct_no_sbb order by dt asc) as first_dt, 
    -- first_value(video + hsd + voice) over (partition by sub_acct_no_sbb order by dt asc) as initial_rgus, 
    -- first_value(delinquency_days) over (partition by sub_acct_no_sbb order by dt asc) as initial_overduedays,
    -- first_value(date(ls_pay_dte_sbb)) over (partition by sub_acct_no_sbb order by dt asc) as initial_oldest_unpaid_bill_dt,
    -- first_value(dt) over (partition by sub_acct_no_sbb order by dt desc) as last_dt, 
    -- first_value(video + hsd + voice) over (partition by sub_acct_no_sbb order by dt desc) as final_rgus,
    -- first_value(delinquency_days) over (partition by sub_acct_no_sbb order by dt desc) as final_overduedays,
    -- first_value(date(ls_pay_dte_sbb)) over (partition by sub_acct_no_sbb order by dt desc) as final_oldest_unpaid_bill_dt
    
    distinct case when A.sub_acct_no_sbb is null then B.sub_acct_no_sbb else A.sub_acct_no_sbb end as account_id, 
    first_value(C.dt) over (partition by C.sub_acct_no_sbb order by C.dt asc) as first_dt,
    A.video + A.hsd + A.voice as initial_rgus, 
    A.delinquency_days as initial_overduedays, 
    A.ls_pay_dte_sbb as intial_oldest_unpaid_bill_dt, 
    first_value(C.dt) over (partition by C.sub_acct_no_sbb order by C.dt desc) as last_dt,
    B.video + B.hsd + B.voice as final_rgus, 
    B.delinquency_days as final_overduedays, 
    B.ls_pay_dte_sbb as final_oldest_unpaid_bill_dt
    
FROM dna_bom A
FULL OUTER JOIN dna_eom B
    ON A.sub_acct_no_sbb = B.sub_acct_no_sbb
FULL OUTER JOIN (SELECT * FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P' and cust_typ_sbb = 'RES' and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)) C
    ON A.sub_acct_no_sbb = C.sub_acct_no_sbb
)

, order_activity as (
SELECT
    ls_chg_dte_ocr as order_date,
    ord_typ as order_type, 
    sub_acct_no_ooi as account_id,
    bef_hsd + bef_video + bef_voice as bef_rgus, 
    aft_hsd + aft_video + aft_voice as aft_rgus, 
    disco_rsn_sbb as order_info
    -- order_rsn as order_info
-- FROM "lcpr.sandbox.dev"."transactions_orderactivity" 
-- FROM "db_stage_dev"."orderactivity_may_2023"
FROM "db_stage_dev"."orderactivity_mar_apr_2023"
WHERE
    date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters) 
    and acct_type = 'R' 
)

, balance as (
SELECT
    distinct account_id, 
    first_value(order_type) over (partition by account_id order by order_date asc) as first_order_type, 
    first_value(order_date) over (partition by account_id order by order_date asc) as first_order_dt,
    first_value(bef_rgus) over (partition by account_id order by order_date asc) as initial_rgus, 
    first_value(order_type) over (partition by account_id order by order_date desc) as last_order_type, 
    first_value(order_date) over (partition by account_id order by order_date desc) as last_order_dt,
    first_value(aft_rgus) over (partition by account_id order by order_date desc) as final_rgus
FROM order_activity
)

, net_movements as (
SELECT
    distinct case
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'CONNECT') and initial_rgus = 0 and final_rgus > initial_rgus then 'CONNECT'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'RESTART') and initial_rgus = 0 and final_rgus > initial_rgus then 'RESTART'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'UPGRADE') and final_rgus > initial_rgus then 'UPGRADE'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'V_DISCO' and order_info != 'VL') and final_rgus = 0 then 'V_DISCO'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'NON PAY' and order_info != 'VL') and final_rgus = 0 and account_id not in (SELECT sub_acct_no_sbb FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P' and cust_typ_sbb = 'RES' and date(dt) = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day and acp is not null) then 'NON PAY'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'DOWNGRADE') and initial_rgus > final_rgus then 'DOWNGRADE'
    else null end as order_type, 
    account_id, 
    initial_rgus as b_rgus, 
    final_rgus as e_rgus
FROM balance
)

, churn_raw as (
SELECT
    account_id, 
    bef_rgus as b_rgus, 
    aft_rgus as e_rgus
FROM order_activity
WHERE
    -- order_type = 'V_DISCO'
    -- order_type = 'NON PAY'
    order_type in ('V_DISCO', 'NON PAY')
)

, churn_net as (
SELECT
    account_id, 
    b_rgus, 
    e_rgus
FROM net_movements
WHERE
    -- order_type = 'V_DISCO'
    -- order_type = 'NON PAY'
    order_type in ('V_DISCO', 'NON PAY')
)


-------------------- RESULTS

-- SELECT
--     count(distinct account_id) as accounts, 
--     sum(b_rgus) as b_rgus
-- FROM churn_net

--- --- Look in Fixed Table
-- SELECT
--     -- fix_s_fla_mainmovement, 
--     fix_s_fla_churntype,
--     count(distinct fix_s_att_account) as cuentas, 
--     sum(fix_b_mes_numrgus) as b_rgus, 
--     sum(fix_e_mes_numrgus) as e_rgus
-- FROM "db_stage_dev"."lcpr_fixed_apr2023"
-- WHERE 
--     fix_s_att_account in (SELECT account_id FROM churn_net)
--     and fix_s_fla_mainmovement = '6.Null last day'
-- GROUP BY 1

--- --- Look in Fixed Table for accounts that are not in Order Activity
-- SELECT
--     -- fix_s_fla_mainmovement, 
--     fix_s_fla_churntype,
--     count(distinct fix_s_att_account) as cuentas, 
--     sum(fix_b_mes_numrgus) as b_rgus, 
--     sum(fix_e_mes_numrgus) as e_rgus
-- FROM "db_stage_dev"."lcpr_fixed_apr2023"
-- WHERE 
--     fix_s_att_account not in (SELECT account_id FROM churn_raw)
--     and fix_s_fla_mainmovement = '6.Null last day'
--     and fix_s_fla_churntype = '2. Fixed Involuntary Churner'
--     and fix_s_att_account not in (SELECT sub_acct_no_sbb FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P' and cust_typ_sbb = 'RES' and date(dt) = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day and acp is not null)
-- GROUP BY 1  


--- --- Look in DNA (and not in Fixed Table)

-- SELECT
--     case 
--         when delinquency_days > 85 then 'overdue > 85'
--         when delinquency_days <= 85 then 'overdue <= 85'
--         when delinquency_days is null then 'null overdue'
--     else null end as overdue_class,
--     acp,
--     count(distinct sub_acct_no_sbb) as accounts, 
--     sum(video + hsd + voice) as RGUs
-- FROM dna_bom
-- WHERE 
--     sub_acct_no_sbb not in (SELECT fix_s_att_account FROM "db_stage_dev"."lcpr_fixed_apr2023")
--     and sub_acct_no_sbb in (SELECT account_id FROM churn_net)
--     and sub_acct_no_sbb not in (SELECT sub_acct_no_sbb FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P' and cust_typ_sbb = 'RES' and date(dt) = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day and acp is not null)
-- GROUP BY 1, 2

--- --- Look in Order Activity
-- SELECT
--     count(distinct account_id) as accounts, 
--     sum(b_rgus) as b_rgus, 
--     sum(e_rgus) as e_rgus
-- FROM churn_net
-- WHERE
--     account_id not in (SELECT sub_acct_no_sbb FROM dna_bom)
--     and account_id not in (SELECT fix_s_att_account FROM "db_stage_dev"."lcpr_fixed_apr2023")
--     and account_id not in (SELECT sub_acct_no_sbb FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P' and cust_typ_sbb = 'RES' and date(dt) = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day and acp is not null)

------------------------ EXAMPLES
SELECT
    A.*, 
    B.* 
    -- ,C.fix_s_fla_mainmovement --- 1. No churners and 6. Recovered intramonth churners
    ,C.fix_s_fla_churntype --- 2. Churners and 5. Not in Order Activity but churners for Oval
FROM dna_summary A
FULL OUTER JOIN balance B
    ON A.account_id = B.account_id
LEFT JOIN (SELECT * FROM "db_stage_dev"."lcpr_fixed_apr2023") C
    ON A.account_id = C.fix_s_att_account
WHERE    

------ 

--- 1. No churners
    -- A.account_id in (SELECT fix_s_att_account FROM "db_stage_dev"."lcpr_fixed_apr2023" WHERE fix_s_fla_mainmovement != '6.Null last day')
    -- and B.account_id in (SELECT account_id FROM churn_net)

--- 2. Churners
    -- A.account_id in (SELECT fix_s_att_account FROM "db_stage_dev"."lcpr_fixed_apr2023" WHERE fix_s_fla_mainmovement = '6.Null last day')
    -- and B.account_id in (SELECT account_id FROM churn_net)
    
-- --- 3. In DNA but not in Fixed Table
--     A.account_id not in (SELECT fix_s_att_account FROM "db_stage_dev"."lcpr_fixed_apr2023")
--     and B.account_id in (SELECT account_id FROM churn_net)

--- 4. Only in Order Activity (not in our data sources)
    -- A.account_id is null
    -- and B.account_id in (SELECT account_id FROM churn_net)
    
--- 5. Not in Order Activity but churners for Oval
    -- A.account_id in (SELECT fix_s_att_account FROM "db_stage_dev"."lcpr_fixed_apr2023" WHERE fix_s_fla_mainmovement = '6.Null last day')
    -- and B.account_id is null
    
--- 6. Recovered intramonth churners
      A.account_id in (SELECT fix_s_att_account FROM "db_stage_dev"."lcpr_fixed_apr2023" WHERE fix_s_fla_mainmovement != '6.Null last day')
      and B.account_id in (SELECT account_id FROM churn_raw)
      and B.account_id not in (SELECT account_id FROM churn_net)
      and B.account_id not in (SELECT account_id FROM order_activity WHERE order_type = 'NON PAY' and order_info != 'VL')
      and B.account_id not in (SELECT sub_acct_no_sbb FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P' and cust_typ_sbb = 'RES' and date(dt) = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day and acp is not null)

------

ORDER BY random(*)
LIMIT 10
