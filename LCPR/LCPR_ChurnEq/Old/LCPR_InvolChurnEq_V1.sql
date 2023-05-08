-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4 - INVOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2023-01-01')) as input_month)

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

, funnel_dna as (
SELECT
    date_trunc('month', date(dt)) as month, 
    date(dt) as dt, 
    date_trunc('month', date(bill_from_dte_sbb)) as billmonth,
    date(bill_from_dte_sbb) as billday,
    sub_acct_no_sbb, 
    delinquency_days as duedays, 
    -- case when delinquency_days = 0 then delinquency_days else date_diff('day', date(bill_from_dte_sbb), date(dt)) end as duedays,
    -- case when delinquency_days = 0 then delinquency_days else delinquency_days - 29 end as duedays,
    22 as firstoverdueday, --- !!! It should actually be 22. but the counter (delinquency_days) goes from 0 directly to 30.
    -- as backlogdate ???
    first_value(delinquency_days) over (partition by sub_acct_no_sbb, date(date_trunc('month', date(dt))) order by date(dt) desc) as lastdueday, 
    bill_from_dte_sbb
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE
    date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
-- Residential and dt for input_month
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Overdueday1 --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, overdueday1 as (
SELECT
    distinct *, 
    case when duedays = firstoverdueday then sub_acct_no_sbb else null end as overdueday1flag
FROM funnel_dna
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Soft Dx --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, softdx as (
SELECT
    distinct *,
    case when duedays = 46 then sub_acct_no_sbb else null end as softdxflag
FROM overdueday1
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Backlog --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, backlog as (
SELECT 
    distinct *,
    case when duedays between (85-(date_diff('day',date_trunc('month', date(dt)),(select input_month from parameters) + interval '1' month - interval '1' day))) and 85 then sub_acct_no_sbb else null end as backlogflag
FROM softdx
)


--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Hard Dx --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, harddx as (
SELECT 
    distinct *, 
    case when duedays >= 85 /* lastdueday >= 85 */ then sub_acct_no_sbb else null end as harddxflag
FROM backlog
)

, fmc_involchurn_flags as (
SELECT
    f.*, 
    H.overdueday1flag, 
    H.softdxflag, 
    H.backlogflag, 
    H.harddxflag
FROM fmc_table_adj F 
LEFT JOIN harddx H
    ON cast(F.fmc_s_att_account as varchar) = cast(H.sub_acct_no_sbb as varchar) and date(F.fmc_s_dim_month) = date(H.month)
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  RGUs --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, flags_all_bb as (
SELECT
    distinct *, 
    case when overdueday1flag is not null and fix_e_fla_bb is not null then overdueday1flag else null end as overdueday1_bb, 
    case when softdxflag is not null and fix_e_fla_bb is not null then softdxflag else null end as softdx_bb,
    case when backlogflag is not null and fix_e_fla_bb is not null then backlogflag else null end as backlog_bb,
    case when harddxflag is not null and fix_e_fla_bb is not null then harddxflag else null end as harddx_bb
FROM fmc_involchurn_flags
)

, flags_all_tv as (
SELECT
    distinct *, 
    case when overdueday1flag is not null and fix_e_fla_tv is not null then overdueday1flag else null end as overdueday1_tv, 
    case when softdxflag is not null and fix_e_fla_tv is not null then softdxflag else null end as softdx_tv,
    case when backlogflag is not null and fix_e_fla_tv is not null then backlogflag else null end as backlog_tv,
    case when harddxflag is not null and fix_e_fla_tv is not null then harddxflag else null end as harddx_tv
FROM flags_all_bb
)

, flags_all_vo as (
SELECT
    distinct *, 
    case when overdueday1flag is not null and fix_e_fla_vo is not null then overdueday1flag else null end as overdueday1_vo, 
    case when softdxflag is not null and fix_e_fla_vo is not null then softdxflag else null end as softdx_vo,
    case when backlogflag is not null and fix_e_fla_vo is not null then backlogflag else null end as backlog_vo,
    case when harddxflag is not null and fix_e_fla_vo is not null then harddxflag else null end as harddx_vo
FROM flags_all_tv
)


--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- ---  Final result --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

SELECT
    count(distinct overdueday1flag) as overdueday1_clients, 
    count(distinct softdxflag) as softdx_clients, 
    count(distinct backlogflag) as backlog_clients, 
    count(distinct harddxflag) as harddx_clients,
    count(distinct overdueday1_bb) as overdueday1_bb,
    count(distinct overdueday1_tv) as overdueday1_tv, 
    count(distinct overdueday1_vo) as overdueday1_vo,
    count(distinct softdx_bb) as softdx_bb,
    count(distinct softdx_tv) as softdx_tv, 
    count(distinct softdx_vo) as softdx_vo,
    count(distinct backlog_bb) as backlog_bb,
    count(distinct backlog_tv) as backlog_tv, 
    count(distinct backlog_vo) as backlog_vo,
    count(distinct harddx_bb) as harddx_bb,
    count(distinct harddx_tv) as harddx_tv, 
    count(distinct harddx_vo) as harddx_vo, 
    count(distinct case when fmc_e_att_active = 1 then fix_s_att_account else null end) as che_s_mes_active_base
FROM flags_all_vo


-- SELECT
--     distinct duedays
-- FROM funnel1_dna
-- ORDER BY duedays asc

-- SELECT 
--     distinct fmc_s_dim_month, 
--     fmc_b_fla_fmcsegment, 
--     fmc_b_fla_fmctype, 
--     fmc_b_fla_tech, 
--     fix_b_fla_tenure, --- Fixed tenure!
--     fmc_e_fla_fmcsegment, 
--     fmc_e_fla_fmctype, 
--     fmc_e_fla_tech, 
--     fix_e_fla_tenure,
--     fix_s_fla_churnflag, 
--     fix_s_fla_churntype, 
--     fix_s_fla_mainmovement, 
--     fmc_s_fla_waterfall, 
    
--     count(distinct fix_s_att_account) as che_s_mes_active_base,
--     count(distinct fix_e_fla_bb) as che_s_mes_total_bb, 
--     count(distinct fix_e_fla_tv) as che_s_mes_total_tv, 
--     count(distinct fix_e_fla_vo) as che_s_mes_total_vo, 
    
--     count(distinct overdueday1flag) as che_s_mes_day1, 
--     count(distinct softdxflag) as che_s_mes_softdx, 
--     count(distinct backlogflag) as che_s_mes_backlog, 
--     count(distinct harddxflag) as che_s_mes_harddx, 
    
--     count(distinct overdueday1_bb) as che_s_mes_overdue1day_bb, 
--     count(distinct softdx_b) as che_s_mes_softdx_bb, 
--     count(distinct backlog_b) AS che_s_mes_backlog_bb, 
--     count(distinct harddx_b) as che_s_mes_harddx_bb, 
    
--     count(distinct overdueday1_tv) as che_s_mes_overdue1day_tv, 
--     count(distinct softdx_tv) as che_s_mes_softdx_tv,
--     count(distinct backlog_tv) AS che_s_mes_backlog_tv, 
--     count(distinct harddx_tv) as che_s_mes_harddx_tv, 
    
--     count(distinct overdueday1_vo) as che_s_mes_overdue1day_vo, 
--     count(distinct softdx_vo) as che_s_mes_softdx_vo, 
--     count(distinct backlog_vo) AS che_s_mes_backlog_vo,
--     count(distinct harddx_vo) as che_s_mes_harddx_vo
    
-- FROM flags_all_vo
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
-- ORDER BY 1


-- SELECT
--     sub_acct_no_sbb,
--     duedays, 
--     dt
-- FROM funnel_dna
-- WHERE sub_acct_no_sbb in (SELECT softdxflag FROM softdx)
-- ORDER BY 1, 2 desc
