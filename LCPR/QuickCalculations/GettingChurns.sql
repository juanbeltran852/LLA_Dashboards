SELECT
    -- distinct fix_s_fla_mainmovement
    -- distinct fix_s_fla_churntype,
    -- fmc_s_fla_churntype,
    count(distinct fix_s_att_account),
    sum(case 
        when fix_s_fla_mainmovement = '3.Downsell' then fix_b_mes_numrgus - fix_e_mes_numrgus
        when fix_s_fla_mainmovement = '6.Null last day' then fix_b_mes_numrgus
        else null end)
    -- *
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev"
WHERE
    fmc_s_dim_month = date('2023-01-01')
    and fmc_b_fla_fmc = 'Fixed Only'
    and fix_s_fla_churntype != '1. Fixed Involuntary Churner'
    -- -- and fmc_s_fla_churntype in ('Voluntary Churner', 'Fixed Transfer')
    and fix_s_fla_mainmovement in ('3.Downsell', '6.Null last day')
-- LIMIT 10
-- GROUP BY 1 --, 2
