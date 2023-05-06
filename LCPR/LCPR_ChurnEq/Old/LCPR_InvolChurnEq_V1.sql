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

, funnel1_dna as (
SELECT
    date_trunc('month', date(dt)) as month, 
    date(dt) as dt, 
    date_trunc('month', date(bill_from_dte_sbb)) as billmonth,
    date(bill_from_dte_sbb) as billday,
    sub_acct_no_sbb, 
    delinquency_days as duedays, 
    30 as firstoverdueday, --- !!!
    -- backlogdate
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
FROM funnel1_dna
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
    case when duedays >= 85 and lastdueday >= 85 then sub_acct_no_sbb else null end as harddxflag
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
    count(distinct harddx_vo) as harddx_vo
FROM flags_all_vo


-- SELECT
--     distinct duedays
-- FROM funnel1_dna
-- ORDER BY duedays asc

-- SELECT 
--     distinct fmc_s_dim_month, 
--     fmc_b_fla_fmcsegment, 
    
-- ,B_FMCSegment as che_b_fla_che_segment,B_FMCType as che_b_fla_che_type ,B_Final_TechFlag as che_b_fla_final_tech,b_fixedtenure as che_b_fla_final_tenure,
-- E_FMCSegment as che_e_fla_che_segment,E_FMCType as che_e_fla_che_type,E_Final_TechFlag as che_e_fla_final_tech,e_fixedtenure as che_e_fla_final_tenure,
-- fixedchurnflag as che_s_fla_churn,fixedchurntype as che_s_fla_churn_type/*fixedchurnsubtype as che_s_fla_churn_subtype*/ ,fixedmainmovement as che_s_dim_main_movement,waterfall_flag as che_s_fla_waterfall

-- ,count(distinct fixedaccount) as che_s_mes_active_base
-- ,count(distinct e_bb) as che_s_mes_total_bb
-- ,count(distinct e_tv) as che_s_mes_total_tv
-- ,count(distinct e_vo) as che_s_mes_total_vo
-- ,count(distinct Overdue1Day) as che_s_mes_day1, count(distinct SoftDx) as che_s_mes_softdx,count(distinct backlog) AS che_s_mes_backlog
-- ,count(distinct harddx) as che_s_mes_harddx 


-- ,count(distinct Overdue1Day_BB) as che_s_mes_overdue1day_bb, count(distinct SoftDx_BB) as che_s_mes_softdx_bb
-- ,count(distinct backlog_BB) AS che_s_mes_backlog_bb,count(distinct harddx_BB) as che_s_mes_harddx_bb, count(distinct Overdue1Day_TV) as che_s_mes_overdue1day_tv, count(distinct SoftDx_TV) as che_s_mes_softdx_tv
-- ,count(distinct backlog_TV) AS che_s_mes_backlog_tv,count(distinct harddx_TV) as che_s_mes_harddx_tv, count(distinct Overdue1Day_VO) as che_s_mes_overdue1day_vo, count(distinct SoftDx_VO) as che_s_mes_softdx_vo
-- ,count(distinct backlog_VO) AS che_s_mes_backlog_vo,count(distinct harddx_VO) as che_s_mes_harddx_vo
-- FROM Cohort_Flag
-- --WHERE Month=date('2022-02-01') and harddx IS NULL AND backlog IS NOT NULL
-- GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
-- --order by users
-- order by 1


-- SELECT * FROM fmc_flags


