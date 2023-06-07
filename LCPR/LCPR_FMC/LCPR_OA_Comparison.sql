WITH

parameters as (SELECT date('2023-04-01') as input_month)

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

, flags_comparison as (
SELECT
    A.*, 
    B.fix_s_fla_mainmovement, 
    case
        when A.initial_rgus = A.final_rgus and A.initial_rgus != 0 and A.final_rgus != 0 then '1.SameRGUs'
        when A.initial_rgus < A.final_rgus and A.initial_rgus != 0 then '2.Upsell'
        when A.initial_rgus > A.final_rgus and A.final_rgus != 0 then '3.Downsell'
        when A.initial_rgus = 0 and A.final_rgus > 0 and first_order_type = 'CONNECT' then '4.New Customer'
        when A.initial_rgus = 0 and A.final_rgus > 0 and first_order_type = 'RESTART' then '5.Come Back to Life'
        when A.initial_rgus > 0 and A.final_rgus = 0 then '6.Null last day'
    else 'Otro' end as manual_mainmovement
FROM customer_summary A
LEFT JOIN fixed_table B
    ON A.account_id = B.fix_s_att_account
ORDER BY num_orders desc
)

SELECT
    case when fix_s_fla_mainmovement = manual_mainmovement then 'Match' else 'Do not match' end as comparison_flag, 
    fix_s_fla_mainmovement,
    count(distinct account_id) as num_clients
FROM flags_comparison
GROUP BY 1, 2
ORDER BY 1, 2, 3

-- SELECT
--     distinct num_orders, 
--     count(distinct account_id) as num_accounts
-- FROM customer_summary
-- GROUP BY 1
-- ORDER BY num_orders desc

-- SELECT
--     *
-- FROM "lcpr.sandbox.dev"."transactions_orderactivity"
-- WHERE 
--     date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters)
--     and acct_type = 'R'
-- --     and sub_acct_no_ooi = 8211060040000483	
--     and sub_acct_no_ooi = 8211080560277451
