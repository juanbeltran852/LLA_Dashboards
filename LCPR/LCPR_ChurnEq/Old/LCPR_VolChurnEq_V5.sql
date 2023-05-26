-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4.2 - VOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2022-12-01')) as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- FMC - Total Voluntary Churn --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, total_vol_churn as (
SELECT
    *,
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
    date_trunc('month', date(B.order_start_date)) = (SELECT input_month FROM parameters)
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

SELECT 
    count(distinct dx_no_cc) + count(distinct ret_account_id) as all_attempts, 
    count(distinct ret_account_id) as attempts_cc, 
    count(distinct dx_no_cc) as dx_no_cc,
    count(distinct retained_flag) as retained_customers, 
    cast(count(distinct retained_flag) as double)/cast(count(distinct ret_account_id) + count(distinct dx_no_cc) as double) as ret_rate_all, 
    cast(count(distinct retained_flag) as double)/cast(count(distinct ret_account_id) as double) as ret_rate_cc,
    count(distinct not_retained_flag) as no_ret,
    count(distinct case when not_retained_flag is not null and vol_dx_flag is not null then ret_account_id else null end) as dx_no_ret, 
    count(distinct case when not_retained_flag is not null and vol_dx_flag is null then ret_account_id else null end) as no_dx_no_ret,
    count(distinct vol_churn_id) as total_voluntary_churn
    
    -- count(distinct case when dx_no_cc is not null or ret_account_id is not null then fix_b_fla_vo else null end) as all_bb,
    -- count(distinct case when ret_account_id is not null then fix_b_fla_vo else null end) as cc_bb, 
    -- count(distinct case when retained_flag is not null then fix_e_fla_bb else null end) as ret_bb
FROM vol_churn_eq




-- SELECT
--     *
-- FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
-- WHERE
--     fmc_s_dim_month = (SELECT input_month FROM parameters)
--     and fmc_s_fla_churntype = 'Voluntary Churner'
--     and fmc_s_fla_churnflag in ('Fixed Churner', 'Churner')
-- LIMIT 10
