--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 3 - OPERATIONAL DRIVERS - MRC CHANGES ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH 

parameters AS (
SELECT date_trunc('month', date('2023-03-01')) as input_month
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- MRC Changes --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, bom_previous_month as (
SELECT 
    fmc_s_dim_month as fmc_s_dim_month_prev, 
    fix_s_att_account, 
    fmc_s_att_account, 
    fix_b_mes_mrc as fix_b_mes_mrc_prev
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters) - interval '1' month  
    and fix_b_mes_overdue < 85
 )

, eom_current_month as (
SELECT
    fmc_s_dim_month, 
    fmc_e_fla_tech,
    fmc_e_fla_fmcsegment,
    fmc_e_fla_fmc,
    fmc_e_fla_tenure,
    fix_s_att_account, 
    fix_e_mes_overdue,
    fix_e_mes_mrc, 
    fix_e_att_active, 
    fmc_s_fla_churnflag, 
    fmc_s_fla_waterfall, 
    fix_s_fla_mainmovement
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters) 
    and fix_e_mes_overdue < 85
)

 , mrc_changes as (
SELECT  
    fmc_s_dim_month, 
    fmc_e_fla_tech,
    fmc_e_fla_fmcsegment,
    fmc_e_fla_fmc,
    fmc_e_fla_tenure,
    c.fix_s_att_account,
    case when fix_e_mes_mrc/fix_b_mes_mrc_prev > 1.05 then c.fix_s_att_account else null end as MRC_increase_flag,
    case when fix_e_mes_mrc/fix_b_mes_mrc_prev <= 1.05 then c.fix_s_att_account else null end as no_plan_change, 
    fix_e_att_active, 
    fmc_s_fla_churnflag, 
    fmc_s_fla_waterfall, 
    fix_s_fla_mainmovement
FROM bom_previous_month p 
LEFT JOIN eom_current_month c 
    ON p.fix_s_att_account = c.fix_s_att_account and p.fmc_s_dim_month_prev + interval '1' month = c.fmc_s_dim_month
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flag --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, flag_mrc_changes as (
SELECT
    F.*
FROM mrc_changes F
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fix_e_att_active = 1 
)

, final_table as (
SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech,
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment,
    fmc_e_fla_fmc as odr_e_fla_fmc_type,
    case --- Making sure the tenures fit with the dashboard ones
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure, ---E_FinalTenureSegment,
    count(distinct MRC_Increase_flag) as opd_s_mes_uni_mrcincrease, 
    count(distinct no_plan_change) as opd_s_mes_uni_mrcnoincrease
FROM flag_mrc_changes
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
)

SELECT * FROM final_table

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tests --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- SELECT 
--     sum(opd_s_mes_uni_mrcincrease), 
--     sum(opd_s_mes_uni_mrcnoincrease)
-- FROM final_table
 
 
