select  fmc_s_dim_month
        -- , case when lower(fmc_b_fla_fmc) like '%near%' then 'Near FMC' when lower(fmc_b_fla_fmc) like '%real%' then 'Real FMC' else fmc_b_fla_fmc end as fmc_b_fla_fmc
        , case when lower(fmc_e_fla_fmc) like '%near%' then 'Near FMC' when lower(fmc_e_fla_fmc) like '%real%' then 'Real FMC' else fmc_e_fla_fmc end as fmc_e_fla_fmc
        -- ,fmc_s_fla_churnflag
        ,count(distinct fmc_s_att_account) as accounts
        ,sum(fmc_b_mes_numRGUS) as BOM_RGUS
        ,sum(fmc_e_mes_numRGUS) as EOM_RGUS
FROM "db_stage_dev"."lcpr_fmc_jan2023"
-- WHERE fmc_s_dim_month = DATE(dt)
GROUP BY 1,2 -- ,4
ORDER BY 1,2 -- ,4
