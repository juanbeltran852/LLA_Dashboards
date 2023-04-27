--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 3 - OPERATIONAL DRIVERS - NEVER PAID ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH

parameters as (SELECT date('2023-02-01') as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- FMC Table --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, fmc_table as (
SELECT
    *
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account,
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null
GROUP BY 1, 2
ORDER BY 3 desc
)

, fmc_table_adj as (
SELECT 
    F.*,
    records_per_user
FROM fmc_table F
LEFT JOIN repeated_accounts R
    ON F.fix_s_att_account = R.fix_s_att_account and F.fmc_s_dim_month = R.fmc_s_dim_month
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- New customers --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, new_customers_pre as (
SELECT
    date_trunc('month', date(dt)) as dna_month,
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by DATE(dt) DESC) as timestamp) as date) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account, 
    -- case when date(bill_from_dte_sbb) = date('0000-12-30') then date('2022-12-30') else date(bill_from_dte_sbb)end as bill_from_dte_sbb, 
    bill_from_dte_sbb,
    --- The total MRC must be calculated summing up the charges for the different fixed services.
    (video_chrg + hsd_chrg + voice_chrg) as fi_tot_mrc_amt,
    delinquency_days,
    dt
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(CONNECT_DTE_SBB)) between (SELECT input_month FROM parameters) and ((SELECT input_month FROM parameters) + interval '3' month)
ORDER BY 1
)

, new_customers as (   
SELECT 
    dna_month,
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account as new_sales_flag,
    fix_s_att_account, 
    bill_from_dte_sbb,
    fi_tot_mrc_amt,
    delinquency_days,
    dt
FROM new_customers_pre
WHERE date_trunc('month', date(fix_b_att_maxstart)) = (SELECT input_month FROM parameters)
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- Never Paid (using Payments Table as in CWP) --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

,bills_of_interest AS (
SELECT 
    dna_month,
    fix_s_att_account,
    --- I take the equivalent to oldest_unpaid_bill because no much more alternative info is available
    first_value(bill_from_dte_sbb) over (partition by fix_s_att_account order by dt asc) as first_bill_created
FROM new_customers
)

, mrc_calculation as (
SELECT
    nc.fix_s_att_account, 
    min(first_bill_created) as first_bill_created, 
    max(fi_tot_mrc_amt) as max_tot_mrc, 
    array_agg(distinct fi_tot_mrc_amt order by fi_tot_mrc_amt desc) as arreglo_mrc
FROM new_customers nc
INNER JOIN bills_of_interest bi
    ON nc.fix_s_att_account = bi.fix_s_att_account 
        and date(nc.dt) between date(first_bill_created) and (date(first_bill_created) + interval '3' month)
GROUP BY nc.fix_s_att_account
)

, first_cycle_info as (
SELECT
    nc.fix_s_att_account, 
    --nc.dna_month, 
    min(fix_b_att_maxstart) as first_installation_date, 
    min(first_bill_created) as first_bill_created, 
    try(array_agg(arreglo_mrc)[1]) as arreglo_mrc, 
    max(delinquency_days) as max_delinquency_days_first_bill, 
    max(max_tot_mrc) as max_mrc_first_bill, 
    count(distinct max_tot_mrc) as diff_mrc
FROM new_customers nc
INNER JOIN mrc_calculation mrcc
    ON nc.fix_s_att_account = mrcc.fix_s_att_account
WHERE 
    date(nc.bill_from_dte_sbb) = date(mrcc.first_bill_created)
GROUP BY nc.fix_s_att_account--, nc.dna_month
)

, payments_basic as (
SELECT  
    account_id as fix_s_att_account_payments, 
    try(array_agg(date(dt) order by date(dt))[1]) as first_payment_date, 
    try(array_agg(cast(payment_amt_usd as double) order by date(dt))[1]) as first_payment_amt, 
    try(filter(array_agg(case when cast(payment_amt_usd as double) >= max_mrc_first_bill then date(dt) else null end order by date(dt)), x -> x is not null)[1]) as first_payment_above_date, 
    try(filter(array_agg(case when cast(payment_amt_usd as double) >= max_mrc_first_bill then cast(payment_amt_usd as double) else null end order by date(dt)), x -> x is not null)[1]) as first_payment_above, 
    array_agg(cast(payment_amt_usd as double) order by date(dt)) as arreglo_pagos, 
    try(array_agg(date(dt) order by date(dt))[1]) as first_pay_date, 
    try(array_agg(date(dt) order by date(dt) desc)[1]) as last_pay_date, 
    array_agg(date(dt) order by date(dt)) as arreglo_pagos_dates, 
    
    round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<30, cast(payment_amt_usd as double),null)),2) as total_payments_30_days
    ,round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<60, cast(payment_amt_usd as double),null)),2) as total_payments_60_days
    ,round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<85, cast(payment_amt_usd as double),null)),2) as total_payments_85_days
    
FROM "lcpr.stage.prod"."payments_lcpr" p
INNER JOIN first_cycle_info as fc
    ON cast(fc.fix_s_att_account as varchar) = cast(p.account_id as varchar)
WHERE 
    date(dt) between (fc.first_bill_created - interval '50' day) and (fc.first_bill_created + interval '85' day)
GROUP BY account_id
)


, npn_85 as (
SELECT
    *, 
    fc.fix_s_att_account as fix_s_att_account_def,
    date_diff('day', first_bill_created, first_payment_above_date) as days_between_payment, 
    case when first_payment_above_date is null then 86 else date_diff('day', first_bill_created, first_payment_above_date) end as fixed_days_unpaid_bill, 
    case
        when total_payments_30_days is null then fc.fix_s_att_account
        when total_payments_30_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_30_flag,
    
    case
        when total_payments_60_days is null then fc.fix_s_att_account
        when total_payments_60_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_60_flag,
    case
        when total_payments_85_days is null then fc.fix_s_att_account
        when total_payments_85_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_85_flag
FROM first_cycle_info fc
LEFT JOIN payments_basic p 
    ON cast(p.fix_s_att_account_payments as varchar) = cast(fc.fix_s_att_account as varchar)
)

-- ,prueba as (

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- ,Never_paid_fmc as (
-- select 
--     a.*, CASE WHEN npn_flag is not null THEN 1 ELSE null END AS NEVER_PAID_FLAG,npn_flag from new_customers_flag a left join summary_by_user b on a.finalaccount = b.act_acct_cd
-- )

, flag_new_customers as (
SELECT
    F.*, 
    new_sales_flag
FROM fmc_table_adj F
LEFT JOIN new_customers I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar)
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    -- and fix_e_att_active = 1
)

, flag_npn_85 as (
SELECT
    F.*, 
    npn_85_flag,
    npn_60_flag,
    npn_30_flag
FROM flag_new_customers F
LEFT JOIN npn_85 I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account_def as varchar)
)


, final_table as (
SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    -- fmc_e_fla_tech as odr_e_fla_final_tech,
    -- fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment,
    -- fmc_e_fla_fmc as odr_e_fla_fmc_type,
    -- case 
    --     when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
    --     when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
    --     when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    -- end as odr_e_fla_final_tenure,
    count(distinct fix_s_att_account) as odr_s_mes_active_base,
    count(distinct new_sales_flag) as opd_s_mes_sales,
    count(distinct  npn_30_flag ) as opd_s_mes_never_paid_30,
    count(distinct npn_60_flag ) as opd_s_mes_never_paid_60,
    count(distinct  npn_85_flag ) as opd_s_mes_never_paid_85
FROM flag_npn_85
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1 --, 2, 3, 4, 5
ORDER BY 1 --, 2, 3, 4, 5
)

SELECT * FROM final_table

