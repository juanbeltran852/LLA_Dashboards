---------------------------------------------------------------------------------
---------------------- NET ADDS IN ORDER ACTIVITY -------------------------------
---------------------------------------------------------------------------------
WITH

parameters as (SELECT date('2023-04-01') as input_month)

, order_activity as (
SELECT
    ls_chg_dte_ocr as order_date,
    ord_typ as order_type, 
    sub_acct_no_ooi as account_id,
    bef_hsd + bef_video + bef_voice as bef_rgus, 
    aft_hsd + aft_video + aft_voice as aft_rgus, 
    disco_rsn_sbb as order_info
FROM "lcpr.sandbox.dev"."transactions_orderactivity" 
WHERE
    date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters) 
    and acct_type = 'R' 
)

, balance as (
SELECT
    distinct account_id, 
    first_value(order_type) over (partition by account_id order by order_date asc) as first_order_type, 
    first_value(bef_rgus) over (partition by account_id order by order_date asc) as initial_rgus,
    first_value(order_type) over (partition by account_id order by order_date desc) as last_order_type, 
    first_value(aft_rgus) over (partition by account_id order by order_date desc) as final_rgus
FROM order_activity
)

--- --- Raw numbers (excludes transfers out and transfers in)

-- SELECT
--     distinct order_type, 
--     count(distinct account_id) as accounts, 
--     sum(bef_rgus) as b_rgus, 
--     sum(aft_rgus) as e_rgus
-- FROM order_activity
-- GROUP BY 1

--- --- Net numbers (considers the month balance)

SELECT
    distinct case
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'CONNECT') and initial_rgus = 0 and final_rgus > initial_rgus then 'CONNECT'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'RESTART') and initial_rgus = 0 and final_rgus > initial_rgus then 'RESTART'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'UPGRADE') and final_rgus > initial_rgus then 'UPGRADE'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'V_DISCO' and order_info != 'VL') and final_rgus = 0 then 'V_DISCO'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'NON PAY') and final_rgus = 0 and account_id not in (SELECT sub_acct_no_sbb FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" WHERE play_type <> '0P' and cust_typ_sbb = 'RES' and date(dt) = (SELECT input_month FROM parameters) and acp is not null) then 'NON PAY'
        when account_id in (SELECT account_id FROM order_activity WHERE order_type = 'DOWNGRADE') and initial_rgus > final_rgus then 'DOWNGRADE'
    else null end as order_type, 
    count(distinct account_id) as accounts, 
    sum(initial_rgus) as b_rgus, 
    sum(final_rgus) as e_rgus
FROM balance
GROUP BY 1
