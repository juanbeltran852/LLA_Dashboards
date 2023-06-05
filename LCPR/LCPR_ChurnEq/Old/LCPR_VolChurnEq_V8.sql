-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4.2 - VOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2023-04-01')) as input_month)


--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- DNA (rgus)  --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, dna_rgus_pre as (
SELECT
    distinct cast(sub_acct_no_sbb as varchar) as dna_id, 
    first_value(hsd) over (partition by sub_acct_no_sbb order by dt asc) as bb_bom, 
    first_value(video) over (partition by sub_acct_no_sbb order by dt asc) as tv_bom, 
    first_value(voice) over (partition by sub_acct_no_sbb order by dt asc) as vo_bom,
    first_value(hsd) over (partition by sub_acct_no_sbb order by dt desc) as bb_eom, 
    first_value(video) over (partition by sub_acct_no_sbb order by dt desc) as tv_eom, 
    first_value(voice) over (partition by sub_acct_no_sbb order by dt desc) as vo_eom
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE
    date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
    AND play_type <> '0P'
    AND cust_typ_sbb = 'RES'
)

, dna_rgus as (
SELECT
    *, 
    (bb_bom + tv_bom + vo_bom) as rgus_bom, 
    (bb_eom + tv_eom + vo_eom) as rgus_eom
FROM dna_rgus_pre
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
    cast(B.account_id as varchar) as vol_dx_flag, 
    case 
        when B.command_id = 'V_DISCO' then 'total' 
        when B.command_id = 'DOWNGRADE' then 'partial' 
    else null end as churn_type
FROM "lcpr.stage.prod"."so_ln_lcpr" B
WHERE 
    date_trunc('month', date(B.completed_date)) = (SELECT input_month FROM parameters)
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
    churn_type, 
    ret_account_id, 
    other_interaction_info10, 
    ret_date, 
    
    case when vol_dx_flag is not null and ret_account_id is null then vol_dx_flag else null end as dx_no_cc, 
    
    case when ret_account_id is not null and other_interaction_info10 = 'Retained Customer' and vol_dx_flag is null then ret_account_id else null end as retained_flag
    
FROM dna_rgus A
LEFT JOIN disconnections B
    ON A.dna_id = B.vol_dx_flag
LEFT JOIN all_attempts C
    ON A.dna_id = C.ret_account_id
)

, fmc_join as (
SELECT
    A.*,
    B.*,
    
    case 
        when fix_s_fla_mainmovement = '3.Downsell' then rgus_bom - rgus_eom
        when fix_s_fla_mainmovement = '6.Null last day' then rgus_bom
    else rgus_bom end as churned_rgus, 
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
    else vo_bom end as churned_vo 
    
FROM dna_join A
LEFT JOIN fmc_table B
    ON cast(A.dna_id as varchar) = cast(B.fix_s_att_account as varchar)
)

, final_result as (
SELECT
    --- FMC flags
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
    
    --- Dummies
    case when dx_no_cc is not null or ret_account_id is not null then 1 else 0 end as all_attempts, 
    case when ret_account_id is not null then 1 else 0 end as cc_attempts, 
    case when retained_flag is not null then 1 else 0 end as retained, 
    case when ret_account_id is not null and retained_flag is null and vol_dx_flag is not null then 1 else 0 end as dx_not_retained, 
    case when ret_account_id is not null and retained_flag is null and vol_dx_flag is null then 1 else 0 end as not_dx_not_retained, 
    case when dx_no_cc is not null then 1 else 0 end as dx_no_cc,
    
    case when churn_type = 'total' then 1 else 0 end as total_churn, 
    case when churn_type = 'partial' then 1 else 0 end as partial_churn,
    
    --- Counts
    count(distinct dna_id) as accounts, 
    sum(case when dna_id is not null then churned_rgus else null end) as churned_rgus,
    sum(case when dna_id is not null then churned_bb else null end) as churned_bb,
    sum(case when dna_id is not null then churned_tv else null end) as churned_tv,
    sum(case when dna_id is not null then churned_vo else null end) as churned_vo

FROM fmc_join
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
ORDER BY 13, 14, 15, 16, 17, 18
)

SELECT
    *
FROM final_result



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




