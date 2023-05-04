-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4 - INVOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2023-03-01')) as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- FMC --- --- --- --- --- --- --- --- --- --- --- ---
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
--- --- --- --- --- --- --- --- --- --- --- --- ---  DNA --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- ,Invol_Funnel_Fields AS(
-- select  
--     *,
--     first_value(LagDueDay_feb) over(partition by act_acct_cd, DATE(DATE_TRUNC('MONTH',dt)) order by date(dt) desc) as LastDueDay_feb
-- from(
--     SELECT 
--         DISTINCT DATE(DATE_TRUNC('MONTH',date(d.dt))) AS Month,
--         date(d.dt) AS dt,
--         DATE(DATE_TRUNC('MONTH',fi_bill_dt_m0)) AS BillMonth,
        -- date(fi_bill_dt_m0) as BillDay,
        -- d.act_acct_cd,
        -- d.fi_outst_age AS DueDays,
--         CASE WHEN ACT_BLNG_CYCL IN('A','B','C') THEN 15 ELSE 28 END AS FirstOverdueDay,
--         case when DATE(DATE_TRUNC('MONTH',date(d.dt)))=date('2022-03-01') then date('2022-03-02') else DATE(DATE_TRUNC('MONTH',date(d.dt))) end as Backlog_Date,
--         first_value(fi_outst_age) over(partition by act_acct_cd,DATE(DATE_TRUNC('MONTH',date(d.dt))) order by date(dt) desc) as LastDueDay,oldest_unpaid_bill_dt,
--         lag(fi_outst_age) over(partition by act_acct_cd order by date(dt) asc) as LagDueDay_feb
--     FROM "db-analytics-prod"."fixed_cwp" d
--     WHERE act_cust_typ_nm = 'Residencial' and date(dt) between (select start_date from parameters ) and (select end_date from parameters )
--     )
-- )

, funnel1_dna as (
SELECT
    date_trunc('montn', date(dt)) as month, 
    date(dt) as dt, 
    date_trunc('month', date(bill_from_dte_sbb)) as billmonth,
    date(bill_from_dte_sbb) as billday,
    sub_acct_no_sbb, 
    delinquency_days as duedays, 
    
)

SELECT
    *
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
LIMIT 100

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Overdueday1 --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---



--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Soft Dx --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---



--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Backlog --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---



--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Hard Dx --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---



--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Final result --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
