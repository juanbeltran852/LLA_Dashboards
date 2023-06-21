-- SELECT 
--     -- *
--     fmc_s_dim_month as month,
--     fmc_b_fla_fmcsegment as bundle, 
--     sum(fmc_e_mes_numrgus) as rgus, 
--     count(distinct fmc_s_att_account) as cuentas
-- FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev" 
-- WHERE 
--     fmc_b_att_active = 1
-- GROUP BY 1 ,2
-- ORDER BY 1, 2
-- limit 10;

SELECT 
    -- *
    fmc_s_dim_month as month,
    fmc_b_fla_fmcsegment as bundle, 
    sum(fmc_b_mes_numrgus) as rgus, 
    count(distinct fmc_s_att_account) as cuentas
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev" 
WHERE 
    fmc_b_att_active = 1
    -- and (fmc_s_fla_waterfall = 'Downsell' or 
    and fmc_s_fla_churnflag <> 'Non Churner'
GROUP BY 1 ,2
ORDER BY 1, 2
