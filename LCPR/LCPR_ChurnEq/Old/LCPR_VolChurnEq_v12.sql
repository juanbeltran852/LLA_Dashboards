-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4.2 - VOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2023-05-01')) as input_month)


--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- DNA (rgus)  --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, dna_bom as (
SELECT
    distinct sub_acct_no_sbb as account_bom, 
    hsd as bb_bom, 
    video as tv_bom, 
    voice as vo_bom, 
    hsd + video + voice as rgus_bom, 
    delinquency_days as overdue_bom
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE
    date(dt) = (SELECT input_month FROM parameters)
    and play_type <> '0P'
    and cust_typ_sbb = 'RES'
)

, dna_eom as (
SELECT
    distinct sub_acct_no_sbb as account_eom, 
    hsd as bb_eom, 
    video as tv_eom, 
    voice as vo_eom, 
    hsd + video + voice as rgus_eom, 
    delinquency_days as overdue_eom
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE
    date(dt) = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day
    and play_type <> '0P'
    and cust_typ_sbb = 'RES'
)

, dna_rgus as (
SELECT
    *, 
    case when account_bom is null then account_eom else account_bom end as dna_id
FROM dna_bom A
FULL OUTER JOIN dna_eom B
    ON A.account_bom = B.account_eom
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- FMC - Total Voluntary Churn --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, fmc_table as (
SELECT
    *
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters) 
    and fmc_b_att_active = 1
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- ---  Disconnection orders (Service Orders approach) --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, disconnections as (
SELECT    
    date(B.order_start_date) as dx_date,
    date(B.completed_date) as dx_end_date,
    cast(B.account_id as varchar) as vol_dx_flag
FROM (
    SELECT 
        order_start_date,  
        completed_date, 
        case when (SELECT input_month FROM parameters) = date('2023-02-01') then date(order_start_date) else date(completed_date) end as workaround_date,
        account_id, 
        command_id, 
        order_status, 
        account_type, 
        cease_reason_desc
    FROM "lcpr.stage.prod"."so_ln_lcpr"
    ) B
WHERE 
    date_trunc('month', date(B.workaround_date)) = (SELECT input_month FROM parameters)
    -- date_trunc('month', date(B.completed_date)) = (SELECT input_month FROM parameters)
    and (B.command_id = 'V_DISCO' or B.command_id = 'DOWNGRADE')
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

, dna_join as (
SELECT
    dna_id,
    bb_bom, tv_bom, vo_bom, rgus_bom, 
    bb_eom, tv_eom, vo_eom, rgus_eom, 
    vol_dx_flag, 
    dx_date,
    dx_end_date,
    ret_account_id, 
    other_interaction_info10, 
    ret_date, 
    
    case when vol_dx_flag is not null and ret_account_id is null then vol_dx_flag else null end as dx_no_cc, 
    
    case when ret_account_id is not null and other_interaction_info10 = 'Retained Customer' and vol_dx_flag is null then ret_account_id else null end as retained_flag
    
FROM dna_rgus A
LEFT JOIN disconnections B
    ON cast(A.dna_id as varchar) = cast(B.vol_dx_flag as varchar)
LEFT JOIN all_attempts C
    ON cast(A.dna_id as varchar) = cast(C.ret_account_id as varchar)
)

, fmc_join as (
SELECT
    A.*,
    B.*,
    case 
        when fix_s_fla_mainmovement = '3.Downsell' then rgus_bom - rgus_eom
        when fix_s_fla_mainmovement = '6.Null last day' then rgus_bom
    else rgus_bom end as num_rgus_diss, 
    case 
        when fix_s_fla_mainmovement = '3.Downsell' then bb_bom - bb_eom
        when fix_s_fla_mainmovement = '6.Null last day' then bb_bom
    else bb_bom end as churned_bb, 
    case 
        when fix_s_fla_mainmovement = '3.Downsell' then tv_bom - tv_eom
        when fix_s_fla_mainmovement = '6.Null last day' then tv_bom
    else tv_bom end as churned_tv,
    case 
        when fix_s_fla_mainmovement = '3.Downsell' then vo_bom - vo_eom
        when fix_s_fla_mainmovement = '6.Null last day' then vo_bom
    else vo_bom end as churned_vo, 
    case when fix_s_fla_mainmovement = '3.Downsell' and ret_account_id is null and vol_dx_flag is null then dna_id else null end as bajas_no_cursadas
    
FROM dna_join A
LEFT JOIN fmc_table B
    ON cast(A.dna_id as varchar) = cast(B.fix_s_att_account as varchar)
)

, final_result_pre as (
SELECT
    distinct dna_id, 
    num_rgus_diss,
    rgus_bom, 
    rgus_eom,
    
    --- FMC flags
    fmc_s_dim_month, 
    fmc_b_fla_tech, 
    fmc_b_fla_fmcsegment, 
    fmc_b_fla_fmc, 
    fmc_e_fla_tech, 
    fmc_e_fla_fmcsegment, 
    fmc_e_fla_fmc, 
    fmc_b_fla_tenure, 
    fmc_e_fla_tenure, 
    fix_s_fla_mainmovement,
    fix_s_fla_churntype,
    
    --- Dummies
    case when dx_no_cc is not null or ret_account_id is not null then 1 else 0 end as all_attempts, 
    case when ret_account_id is not null then 1 else 0 end as cc_attempts, 
    case when retained_flag is not null then 1 else 0 end as retained, 
    case when ret_account_id is not null and retained_flag is null and vol_dx_flag is not null then 1 else 0 end as dx_not_retained, 
    case when ret_account_id is not null and retained_flag is null and vol_dx_flag is null then 1 else 0 end as not_dx_not_retained, 
    case 
        when fix_s_fla_mainmovement = '6.Null last day' and dx_no_cc is not null then 1
        when fix_s_fla_mainmovement = '3.Downsell' and (dx_no_cc is not null or bajas_no_cursadas is not null) then 1
    else 0 end as dx_no_cc,
    
    --- Flags for Dashboard
    'Voluntary' as  dx_type, 
    (SELECT input_month FROM parameters) as dt

FROM fmc_join
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17--, 18
-- ORDER BY 13, 14, 15, 16, 17--, 18
)

, final_result as (
SELECT
    --- FMC flags
    distinct fmc_s_dim_month, 
    dt,
    fmc_b_fla_tech, 
    fmc_b_fla_fmcsegment, 
    fmc_b_fla_fmc, 
    fmc_e_fla_tech, 
    fmc_e_fla_fmcsegment, 
    fmc_e_fla_fmc, 
    fmc_b_fla_tenure, 
    fmc_e_fla_tenure, 
    
    --- Dashboard flags
    dx_type, 
    
    --- Vol Churn Eq flags
    cc_attempts, 
    dx_no_cc, 
    all_attempts as intention_id_count,
    
    --- Accounts
    count(distinct dna_id*retained) as retained,
    
    --- RGUs
    sum(num_rgus_diss) as num_rgus_diss,
    sum(rgus_bom) as rgus_bom,
    sum(rgus_eom) as rgus_eom
    
FROM final_result_pre
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    
)


SELECT
    *
FROM final_result

-- LIMIT 10

--- ### ### ### Quick check

--- ### Accounts

-- SELECT
--     sum(accounts*all_attempts) as all_attempts,
--     sum(accounts*cc_attempts) as cc_attempts, 
--     sum(accounts*retained) as retained,
--     sum(accounts*dx_not_retained) as dx_not_ret,
--     sum(accounts*not_dx_not_retained) as not_dx_not_retained,
--     sum(accounts*dx_no_cc) as dx_no_cc
-- FROM final_result

--- ### RGUs 

--- All

-- SELECT
--     sum(churned_rgus*all_attempts) as all_attempts,
--     sum(churned_rgus*cc_attempts) as cc_attempts, 
--     sum(churned_rgus*retained) as retained,
--     sum(churned_rgus*dx_not_retained) as dx_not_ret,
--     sum(churned_rgus*not_dx_not_retained) as not_dx_not_retained,
--     sum(churned_rgus*dx_no_cc) as dx_no_cc, 
--     -- sum(churned_rgus*bajas_no_cursadas) as bajas_no_cursadas
-- FROM final_result

--- BB

-- SELECT
--     sum(churned_bb*all_attempts) as all_attempts,
--     sum(churned_bb*cc_attempts) as cc_attempts, 
--     sum(churned_bb*retained) as retained,
--     sum(churned_bb*dx_not_retained) as dx_not_ret,
--     sum(churned_bb*not_dx_not_retained) as not_dx_not_retained,
--     sum(churned_bb*dx_no_cc) as dx_no_cc,
--     sum(churned_bb*bajas_no_cursadas) as bajas_no_cursadas
-- FROM final_result

--- TV 

-- SELECT
--     sum(churned_tv*all_attempts) as all_attempts,
--     sum(churned_tv*cc_attempts) as cc_attempts, 
--     sum(churned_tv*retained) as retained,
--     sum(churned_tv*dx_not_retained) as dx_not_ret,
--     sum(churned_tv*not_dx_not_retained) as not_dx_not_retained,
--     sum(churned_tv*dx_no_cc) as dx_no_cc,
--     sum(churned_tv*bajas_no_cursadas) as bajas_no_cursadas
-- FROM final_result

--- VO

-- SELECT
--     sum(churned_vo*all_attempts) as all_attempts,
--     sum(churned_vo*cc_attempts) as cc_attempts, 
--     sum(churned_vo*retained) as retained,
--     sum(churned_vo*dx_not_retained) as dx_not_ret,
--     sum(churned_vo*not_dx_not_retained) as not_dx_not_retained,
--     sum(churned_vo*dx_no_cc) as dx_no_cc, 
--     sum(churned_vo*bajas_no_cursadas) as bajas_no_cursadas
-- FROM final_result


--- ### Power Bi calculations

-- SELECT
    -- sum(cc_attempts*rgus_bom) as intent_to_churn_cc, 
    
    -- sum(if(cc_attempts = 1 and (dx_type = 'Voluntary' or dx_type = '1. Fixed Voluntary Churner'), num_rgus_diss*intention_id_count, 0)) as completed_churn_orders_cc, 
    
    -- sum(if(dx_no_cc = 1, rgus_bom*intention_id_count)) as non_completed_churn_orders_cc
    
    -- sum(intention_id_count) - sum()
-- FROM final_result
