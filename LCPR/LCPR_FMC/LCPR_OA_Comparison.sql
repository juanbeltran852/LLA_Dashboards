WITH

parameters as (SELECT date('2023-04-01') as input_month)

, accounts_of_analysis as (
SELECT
    sub_acct_no_ooi as account_id
FROM "lcpr.sandbox.dev"."transactions_orderactivity"
WHERE 
    date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters)
    and acct_type = 'R'
    -- and ord_typ = 'V_DISCO'
    and ord_typ = 'NON PAY'
)

, order_activity as (
SELECT
    -- *, 
    -- row_number() over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr asc) as r_nm
    
    distinct sub_acct_no_ooi as account_id, 
    first_value(ord_typ) over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr asc) as first_order_type, 
    first_value(ls_chg_dte_ocr) over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr asc) as first_order_dt,
    
    first_value(bef_video + bef_hsd + bef_voice) over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr asc) as initial_rgus, 
    
    first_value(ord_typ) over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr desc) as last_order_type, 
    first_value(ls_chg_dte_ocr) over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr desc) as last_order_dt,
    
    first_value(aft_video + aft_hsd + aft_voice) over (partition by sub_acct_no_ooi order by ls_chg_dte_ocr desc) as final_rgus
    
FROM "lcpr.sandbox.dev"."transactions_orderactivity"
WHERE 
    date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters)
    and acct_type = 'R'
)

, orders_count as (
SELECT
    sub_acct_no_ooi as account_id,
    count(distinct order_no_ooi) as num_orders
FROM "lcpr.sandbox.dev"."transactions_orderactivity"
WHERE 
    date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters)
    and acct_type = 'R'
GROUP BY 1
)

, customer_summary as (
SELECT
    A.*, 
    B.num_orders
FROM order_activity A
LEFT JOIN orders_count B
    ON A.account_id = B.account_id
ORDER BY B.num_orders desc
)

, fixed_table as (
SELECT
    *
FROM "db_stage_dev"."lcpr_fixed_apr2023"
)

, dna as (
SELECT
    distinct sub_acct_no_sbb as account_id, 
    first_value(dt) over (partition by sub_acct_no_sbb order by dt asc) as first_dt, 
    first_value(video + hsd + voice) over (partition by sub_acct_no_sbb order by dt asc) as initial_rgus, 
    first_value(delinquency_days) over (partition by sub_acct_no_sbb order by dt asc) as initial_overduedays,
    first_value(date(ls_pay_dte_sbb)) over (partition by sub_acct_no_sbb order by dt asc) as initial_oldest_unpaid_bill_dt,
    first_value(dt) over (partition by sub_acct_no_sbb order by dt desc) as last_dt, 
    first_value(video + hsd + voice) over (partition by sub_acct_no_sbb order by dt desc) as final_rgus,
    first_value(delinquency_days) over (partition by sub_acct_no_sbb order by dt desc) as final_overduedays,
    first_value(date(ls_pay_dte_sbb)) over (partition by sub_acct_no_sbb order by dt desc) as final_oldest_unpaid_bill_dt
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
)

, final_join as (
SELECT
    -- case 
    --     when A.account_id is null then C.fix_s_att_account
    --     when A.account_id is null and C.fix_s_att_account is null then B.account_id
    -- else A.account_id end as final_account,
    A.*, 
    -- B.account_id as b_id,
    B.first_dt, 
    B.initial_rgus,
    B.initial_overduedays,
    B.initial_oldest_unpaid_bill_dt, 
    B.last_dt, 
    B.final_rgus, 
    B.final_overduedays, 
    B.final_oldest_unpaid_bill_dt,
    -- C.fix_s_att_account,
    C.fix_s_fla_mainmovement,
    C.fix_s_fla_churntype, 
    case when A.initial_rgus != B.initial_rgus or A.final_rgus != B.final_rgus then '*' else null end as check
FROM customer_summary A
LEFT OUTER JOIN dna B
    ON A.account_id = B.account_id
FULL OUTER JOIN fixed_table C
    ON A.account_id = C.fix_s_att_account
)

SELECT
    *
    -- count(distinct account_id),
    -- count(distinct b_id),
    -- count(distinct fix_s_att_account)
FROM final_join
WHERE
    fix_s_fla_mainmovement = '1.SameRGUs' -- and fix_s_fla_churntype = '2. Fixed Involuntary Churner'
    -- b_id is null and fix_s_att_account is null -- and first_dt is null
    -- and account_id is null
    and account_id in (SELECT account_id FROM accounts_of_analysis)
ORDER BY random(*)
LIMIT 10

-- SELECT
--     'Involuntary churn' as OpCo_flag,
--     fix_s_fla_mainmovement as Oval_flag,
--     count(distinct account_id)
-- FROM final_join
-- WHERE check is not null
--     and account_id in (SELECT account_id FROM accounts_of_analysis)
-- GROUP BY 1, 2
-- Order by 2

-- SELECT
--     *
-- FROM customer_summary
-- WHERE
--     account_id not in (SELECT fix_s_att_account FROM fixed_table)
--     and account_id not in (SELECT account_id FROM dna)
-- LIMIT 10

-- SELECT
--     A.*, 
--     B.fix_s_fla_mainmovement,
--     B.fix_s_fla_churntype
-- FROM dna A
-- LEFT JOIN fixed_table B
--     ON A.account_id = B.fix_s_att_account
-- WHERE A.account_id not in (SELECT account_id FROM customer_summary)
--     and fix_s_fla_churntype = '2. Fixed Involuntary Churner'
-- LIMIT 10

-- SELECT
--     *
-- FROM final_join
-- WHERE final_account = 8211990040242740


-- , flags_comparison as (
-- SELECT
--     A.*, 
--     B.fix_s_fla_mainmovement, 
--     case
--         when A.initial_rgus = A.final_rgus and A.initial_rgus != 0 and A.final_rgus != 0 then '1.SameRGUs'
--         when A.initial_rgus < A.final_rgus and A.initial_rgus != 0 then '2.Upsell'
--         when A.initial_rgus > A.final_rgus and A.final_rgus != 0 then '3.Downsell'
--         when A.initial_rgus = 0 and A.final_rgus > 0 and first_order_type = 'CONNECT' then '4.New Customer'
--         when A.initial_rgus = 0 and A.final_rgus > 0 and first_order_type = 'RESTART' then '5.Come Back to Life'
--         when A.initial_rgus > 0 and A.final_rgus = 0 then '6.Null last day'
--     else 'Otro' end as manual_mainmovement
-- FROM customer_summary A
-- LEFT JOIN fixed_table B
--     ON A.account_id = B.fix_s_att_account
-- ORDER BY num_orders desc
-- )


-- SELECT
--     case when fix_s_fla_mainmovement = manual_mainmovement then 'Match' else 'Do not match' end as comparison_flag, 
--     fix_s_fla_mainmovement,
--     count(distinct account_id) as num_clients
-- FROM flags_comparison
-- GROUP BY 1, 2
-- ORDER BY 1, 2, 3

-- SELECT
--     distinct num_orders, 
--     count(distinct account_id) as num_accounts
-- FROM customer_summary
-- GROUP BY 1
-- ORDER BY num_orders desc

-- SELECT
--     -- *
--     dt,
--     sub_acct_no_sbb as account_id, 
--     (video + hsd + voice) as num_rgus,
--     date(bill_from_dte_sbb) as oldest_unpaid_bill_dt, 
--     delinquency_days
-- FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
-- WHERE
--     play_type <> '0P'
--     and cust_typ_sbb = 'RES' 
--     and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
--     -- and sub_acct_no_sbb = 8211080740348289
--     and sub_acct_no_sbb = 8211990040242745
-- ORDER BY dt

-- SELECT
--     -- *
--     ls_chg_dte_ocr as dt, 
--     (bef_video + bef_hsd + bef_voice) as rgus_before, 
--     (aft_video + aft_hsd + aft_voice) as rgus_after, 
--     ord_typ as order_type
-- FROM "lcpr.sandbox.dev"."transactions_orderactivity"
-- WHERE 
--     date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters)
--     and acct_type = 'R'	
--     -- and sub_acct_no_ooi = 8211080740348289 
--     -- and sub_acct_no_ooi = 8211990040242745
-- ORDER BY 1 asc
