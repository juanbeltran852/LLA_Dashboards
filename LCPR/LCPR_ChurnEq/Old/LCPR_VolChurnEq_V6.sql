-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4.2 - VOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2023-02-01')) as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- FMC - Total Voluntary Churn --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, fmc_table as (
SELECT
    *, 
    case when fix_b_fla_bb is not null then 1 else 0 end as bb_bom, 
    case when fix_b_fla_tv is not null then 1 else 0 end as tv_bom, 
    case when fix_b_fla_vo is not null then 1 else 0 end as vo_bom, 
    case when fix_e_fla_bb is not null then 1 else 0 end as bb_eom, 
    case when fix_e_fla_tv is not null then 1 else 0 end as tv_eom, 
    case when fix_e_fla_vo is not null then 1 else 0 end as vo_eom
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters) 
    and fmc_b_att_active = 1
)


, total_vol_churn as (
SELECT
    cast(fix_s_att_account as varchar) as vol_churn_id,
    fmc_s_fla_churntype as churntype
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fmc_s_fla_churntype = 'Voluntary Churner'
    and fmc_s_fla_churnflag in ('Fixed Churner', 'Churner')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- ---  Disconnection orders (Service Orders approach) --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- , vol_disco as (
-- SELECT
--     -- date_trunc('month', date(completed_date)) as dx_month,
--     A.vol_churn_id,
--     date(B.order_start_date) as dx_date,
--     cast(B.account_id as varchar) as vol_dx_flag
    -- cast(lob_bb_count as int) as bb_churn, 
    -- cast(lob_tv_count as int) as tv_churn, 
    -- cast(lob_vo_count as int) as vo_churn, 
    -- cast((lob_bb_count + lob_tv_count + lob_vo_count) as int) as total_rgus_churn
-- FROM total_vol_churn A
-- FUll OUTER JOIN "lcpr.stage.prod"."so_hdr_lcpr" B
--     ON cast(A.vol_churn_id as varchar) = cast(B.account_id as varchar)
-- WHERE 
--     date_trunc('month', date(B.completed_date)) = (SELECT input_month FROM parameters)
--     and (B.order_type = 'V_DISCO' or lower(B.order_type) like '%dwn%') --- Downgrades are a way in which RGUs churn voluntarily
--     and B.account_type = 'RES'
--     and B.order_status = 'COMPLETE'
--     and B.cease_reason_desc not in ('MIG COAX TO FIB', 'NON-PAY')
-- )

, disconnections as (
SELECT    
    date(B.order_start_date) as dx_date,
    date(B.completed_date) as dx_end_date,
    cast(B.account_id as varchar) as vol_dx_flag
FROM "lcpr.stage.prod"."so_hdr_lcpr" B
WHERE 
    -- date_trunc('month', date(B.completed_date)) = (SELECT input_month FROM parameters)
    date_trunc('month', date(B.completed_date)) = (SELECT input_month FROM parameters)
    and (B.order_type = 'V_DISCO' or lower(B.order_type) like '%dwn%') --- Downgrades are a way in which RGUs churn voluntarily
    and B.account_type = 'RES'
    and B.order_status = 'COMPLETE'
    and B.cease_reason_desc not in ('MIG COAX TO FIB', 'NON-PAY')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- ---  Customers with retentions (Interactions approach) --- --- --- --- --- --- 
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, all_attempts as (
SELECT
    B.account_id as ret_account_id,
    B.other_interaction_info10, 
    date(interaction_start_time) as ret_date
FROM "lcpr.stage.prod"."lcpr_interactions_csg" B
WHERE
    date_trunc('month', date(B.interaction_start_time)) = (SELECT input_month FROM parameters)
    and B.interaction_status = 'Closed'
    and B.other_interaction_info10 in ('Retained Customer', /*'Retention',*/ 'Not Retained')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- ---  Voluntary Churn Equation Flags --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, vol_churn_eq as (
SELECT
    A.*,
    B.vol_dx_flag,
    C.ret_account_id,
    case when C.other_interaction_info10 = 'Retained Customer' and B.vol_dx_flag is null then C.ret_account_id else null end as retained_flag,
    case when (C.other_interaction_info10 = 'Retained Customer' and B.vol_dx_flag is not null) or (C.other_interaction_info10 = 'Not Retained') then C.ret_account_id else null end as not_retained_flag,
    case when B.vol_dx_flag is not null and C.ret_account_id is null then B.vol_dx_flag else null end as dx_no_cc, 
    B.dx_date, 
    B.dx_end_date
FROM total_vol_churn A
FULL OUTER JOIN disconnections B
    ON cast(A.vol_churn_id as varchar) = cast(B.vol_dx_flag as varchar)
FULL OUTER JOIN all_attempts C
    ON B.vol_dx_flag = C.ret_account_id
)

, rejoin_to_fmc as (
SELECT
    distinct *
FROM fmc_table A
LEFT JOIN vol_churn_eq B
    ON cast(A.fix_s_att_account as varchar) = cast(B.vol_dx_flag as varchar)
)

, final_results as (
SELECT
    distinct fmc_s_dim_month, 
    fmc_b_fla_tech, 
    fmc_b_fla_fmcsegment, 
    fmc_b_fla_fmc, 
    fmc_e_fla_tech, 
    fmc_e_fla_fmcsegment, 
    fmc_e_fla_fmc, 
    fmc_b_fla_tenure, 
    fmc_e_fla_tenure, 
    fix_b_fla_tenure, 
    fix_e_fla_tenure, 
    fix_s_fla_churntype, 
    case when ret_account_id is not null then 1 else 0 end as rcoe, --- Dummy for retained customers
    case when fix_b_mes_numrgus is null then 0 else fix_b_mes_numrgus end as b_numrgus, 
    case when fix_e_mes_numrgus is null then 0 else fix_e_mes_numrgus end as e_numrgus, 
    case 
        when retained_flag is not null then '1. Retained' 
        when not_retained_flag is not null then '2. Not retained' 
    else null end as ret_flag_users, 
    case 
        when not_retained_flag is not null and vol_dx_flag is not null then '1. RCOE Dx'
        when not_retained_flag is not null and vol_dx_flag is null then '2. "Baja No Cursada'
    else null end as not_ret_flag_users, 
    case when retained_flag is not null then (bb_bom + tv_bom + vo_bom) else null end as ret_flag_rgus,
    case when not_retained_flag is not null then (bb_bom + tv_bom + vo_bom) else null end as not_ret_flag_rgus,
    bb_bom, 
    tv_bom, 
    vo_bom, 
    bb_eom, 
    tv_eom, 
    vo_eom, 
    count(distinct dx_no_cc) + count(distinct ret_account_id) as all_attempts, 
    count(distinct ret_account_id) as rcoe_attempts, 
    count(distinct vol_dx_flag) as all_real_dx, 
    count(distinct case when not_retained_flag is not null and vol_dx_flag is not null then ret_account_id else null end) as rcoe_real_dx, 
    count(distinct dx_no_cc) as other_vol_dx, 
    count(distinct case when not_retained_flag is not null and vol_dx_flag is null then ret_account_id else null end) as bajasnocursadas, 
    count(distinct retained_flag) as ret_users
FROM rejoin_to_fmc
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
)

SELECT * FROM final_results ORDER BY 1 desc


--- --- --- ### ### ### Quick checks ### ### ### --- --- ---

--- --- --- Accounts
-- SELECT 
--     count(distinct dx_no_cc) + count(distinct ret_account_id) as all_attempts, 
--     count(distinct ret_account_id) as attempts_cc, 
--     count(distinct dx_no_cc) as dx_no_cc,
--     count(distinct retained_flag) as retained_customers, 
--     cast(count(distinct retained_flag) as double)/cast(count(distinct ret_account_id) + count(distinct dx_no_cc) as double) as ret_rate_all, 
--     cast(count(distinct retained_flag) as double)/cast(count(distinct ret_account_id) as double) as ret_rate_cc,
--     count(distinct not_retained_flag) as no_ret,
--     count(distinct case when not_retained_flag is not null and vol_dx_flag is not null then ret_account_id else null end) as dx_no_ret, 
--     count(distinct case when not_retained_flag is not null and vol_dx_flag is null then ret_account_id else null end) as no_dx_no_ret,
--     count(distinct vol_churn_id) as total_voluntary_churn
    
--     -- count(distinct case when dx_no_cc is not null or ret_account_id is not null then fix_b_fla_vo else null end) as all_bb,
--     -- count(distinct case when ret_account_id is not null then fix_b_fla_vo else null end) as cc_bb, 
--     -- count(distinct case when retained_flag is not null then fix_e_fla_bb else null end) as ret_bb
-- FROM vol_churn_eq


--- --- --- RGUs
-- SELECT 
--     sum(case when dx_no_cc is not null or ret_account_id is not null then (bb_bom + tv_bom + vo_bom) else null end) as all_attempts_rgus, 
--     sum(case when ret_account_id is not null then (bb_bom + tv_bom + vo_bom) else null end) as attempts_cc_rgus, 
--     sum(case when retained_flag is not null then (bb_bom + tv_bom + vo_bom) else null end) as retained_rgus, 
--     sum(case when not_retained_flag is not null and vol_dx_flag is not null then (bb_bom + tv_bom + vo_bom) else null end) as dx_no_ret_rgus, 
--     sum(case when not_retained_flag is not null and vol_dx_flag is null then (bb_bom + tv_bom + vo_bom) else null end) as no_dx_no_ret_rgus, 
--     sum(case when dx_no_cc is not null then (bb_bom + tv_bom + vo_bom) else null end) as dx_no_cc_rgus
-- FROM rejoin_to_fmc

