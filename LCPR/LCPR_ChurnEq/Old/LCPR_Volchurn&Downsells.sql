WITH

parameters as (SELECT date('2023-05-01') as input_month)

, pre_fmc as (
SELECT
    fmc_s_dim_month, 
    fix_s_fla_mainmovement, 
    fix_s_fla_churntype, 
    fix_s_att_account, 
    case when fix_b_fla_bb is not null then 1 else 0 end as fix_b_fla_bb, 
    case when fix_b_fla_tv is not null then 1 else 0 end as fix_b_fla_tv, 
    case when fix_b_fla_vo is not null then 1 else 0 end as fix_b_fla_vo, 
    case when fix_e_fla_bb is not null then 1 else 0 end as fix_e_fla_bb, 
    case when fix_e_fla_tv is not null then 1 else 0 end as fix_e_fla_tv, 
    case when fix_e_fla_vo is not null then 1 else 0 end as fix_e_fla_vo, 
    fix_b_mes_numrgus, 
    fix_e_mes_numrgus
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev" 
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
)

SELECT 
    count(distinct fix_s_att_account) as accounts, 
    sum(case 
        when fix_s_fla_mainmovement = '3.Downsell' then fix_b_mes_numrgus - fix_e_mes_numrgus
        when fix_s_fla_mainmovement = '6.Null last day' and fix_s_fla_churntype = '1. Fixed Voluntary Churner' then fix_b_mes_numrgus
    else null end) as rgus,
    sum(case 
        when fix_s_fla_mainmovement = '3.Downsell' then fix_b_fla_bb - fix_e_fla_bb
        when fix_s_fla_mainmovement = '6.Null last day' and fix_s_fla_churntype = '1. Fixed Voluntary Churner' then fix_b_fla_bb
    else null end) as bb,
    sum(case 
        when fix_s_fla_mainmovement = '3.Downsell' then fix_b_fla_tv - fix_e_fla_tv
        when fix_s_fla_mainmovement = '6.Null last day' and fix_s_fla_churntype = '1. Fixed Voluntary Churner' then fix_b_fla_tv
    else null end) as tv,
    sum(case 
        when fix_s_fla_mainmovement = '3.Downsell' then fix_b_fla_vo - fix_e_fla_vo
        when fix_s_fla_mainmovement = '6.Null last day' and fix_s_fla_churntype = '1. Fixed Voluntary Churner' then fix_b_fla_vo
    else null end) as vo
FROM pre_fmc
WHERE
    (fix_s_fla_mainmovement = '3.Downsell' or (fix_s_fla_mainmovement = '6.Null last day' and fix_s_fla_churntype = '1. Fixed Voluntary Churner'))
