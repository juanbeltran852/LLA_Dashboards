-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4.2 - VOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2023-03-01')) as input_month)

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

-- , total_vol_churn as (
-- SELECT
--     cast(sub_acct_no_sbb as varchar) as vol_churn_id, 
--     (hsd + video + voice) as rgus
-- FROM 
-- WHERE
--     date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
--     AND play_type <> '0P'
--     AND cust_typ_sbb = 'RES'
-- )

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- ---  Disconnection orders (Service Orders approach) --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---




, disconnections as (
SELECT    
    date(B.order_start_date) as dx_date,
    date(B.completed_date) as dx_end_date,
    cast(B.account_id as varchar) as vol_dx_flag
FROM "lcpr.stage.prod"."so_hdr_lcpr" B
WHERE 
    date_trunc('month', date(B.order_start_date)) = (SELECT input_month FROM parameters)
    -- date_trunc('month', date(B.completed_date)) = (SELECT input_month FROM parameters)
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
    case 
        when A.vol_churn_id is not null then A.vol_churn_id
        when B.vol_dx_flag is not null then B.vol_dx_flag
        when C.ret_account_id is not null then C.ret_account_id
    end as churn_eq_id,
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
    *
FROM fmc_table A
RIGHT JOIN vol_churn_eq B
    ON cast(A.fix_s_att_account as varchar) = cast(B.vol_dx_flag as varchar)
)

, join_to_dna as (
SELECT
    A.*, 
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
FROM rejoin_to_fmc A
LEFT JOIN dna_rgus B
    ON A.churn_eq_id = B.dna_id
)

, final_table as (
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
    case when dx_no_cc is not null or ret_account_id is not null then 1 else 0 end as all_attempts, 
    case when ret_account_id is not null then 1 else 0 end as cc_attempts, 
    case when retained_flag is not null then 1 else 0 end as retained, 
    case when not_retained_flag is not null and vol_dx_flag is not null then 1 else 0 end as dx_not_retained, 
    case when not_retained_flag is not null and vol_dx_flag is null then 1 else 0 end as not_dx_not_retained, 
    case when dx_no_cc is not null then 1 else 0 end as dx_no_cc,
    count(distinct churn_eq_id) as accounts, 
    sum(case when churn_eq_id is not null then churned_rgus else null end) as churned_rgus,
    sum(case when churn_eq_id is not null then churned_bb else null end) as churned_bb,
    sum(case when churn_eq_id is not null then churned_tv else null end) as churned_tv,
    sum(case when churn_eq_id is not null then churned_vo else null end) as churned_vo
FROM join_to_dna
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
ORDER BY 13, 14, 15, 16, 17, 18
)

SELECT
    *
FROM final_table


--- --- --- ### ### ### Quick check


--- --- --- Accounts
-- SELECT
--     sum(case when all_attempts = 1 then accounts else null end) as all_attempts, 
--     sum(case when cc_attempts = 1 then accounts else null end) as cc_attempts, 
--     sum(case when retained = 1 then accounts else null end) as retained, 
--     sum(case when dx_not_retained = 1 then accounts else null end) as dx_not_retained, 
--     sum(case when not_dx_not_retained = 1 then accounts else null end) as not_dx_not_retained, 
--     sum(case when dx_no_cc = 1 then accounts else null end) as dx_no_cc
-- FROM final_table

--- --- --- RGUs
-- SELECT
--     sum(case when all_attempts = 1 then churned_rgus else null end) as all_attempts_rgus,
--     sum(case when cc_attempts = 1 then churned_rgus else null end) as cc_attempts, 
--     sum(case when retained = 1 then churned_rgus else null end) as retained, 
--     sum(case when dx_not_retained = 1 then churned_rgus else null end) as dx_not_retained, 
--     sum(case when not_dx_not_retained = 1 then churned_rgus else null end) as not_dx_not_retained, 
--     sum(case when dx_no_cc = 1 then churned_rgus else null end) as dx_no_cc
-- FROM final_table

-- SELECT
--     sum(case when all_attempts = 1 then churned_bb else null end) as all_attempts_bb,
--     sum(case when cc_attempts = 1 then churned_bb else null end) as cc_attempts_bb, 
--     sum(case when retained = 1 then churned_bb else null end) as retained_bb, 
--     sum(case when dx_not_retained = 1 then churned_bb else null end) as dx_not_retained_bb, 
--     sum(case when not_dx_not_retained = 1 then churned_bb else null end) as not_dx_not_retained_bb, 
--     sum(case when dx_no_cc = 1 then churned_bb else null end) as dx_no_cc_bb
-- FROM final_table

-- SELECT
--     sum(case when all_attempts = 1 then churned_tv else null end) as all_attempts_tv,
--     sum(case when cc_attempts = 1 then churned_tv else null end) as cc_attempts_tv, 
--     sum(case when retained = 1 then churned_tv else null end) as retained_tv, 
--     sum(case when dx_not_retained = 1 then churned_tv else null end) as dx_not_retained_tv, 
--     sum(case when not_dx_not_retained = 1 then churned_tv else null end) as not_dx_not_retained_tv, 
--     sum(case when dx_no_cc = 1 then churned_tv else null end) as dx_no_cc_tv
-- FROM final_table

-- SELECT
--     sum(case when all_attempts = 1 then churned_vo else null end) as all_attempts_vo,
--     sum(case when cc_attempts = 1 then churned_vo else null end) as cc_attempts_vo, 
--     sum(case when retained = 1 then churned_vo else null end) as retained_vo, 
--     sum(case when dx_not_retained = 1 then churned_vo else null end) as dx_not_retained_vo, 
--     sum(case when not_dx_not_retained = 1 then churned_vo else null end) as not_dx_not_retained_vo, 
--     sum(case when dx_no_cc = 1 then churned_vo else null end) as dx_no_cc_vo
-- FROM final_table
