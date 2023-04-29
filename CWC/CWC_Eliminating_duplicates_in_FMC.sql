SELECT 
    month, 
    case 
        when num_row > 1 and final_account like '%-%' then mobile_account
    else final_account end as final_account,
    case when num_row > 1 and b_fmc_status = 'Soft/Hard FMC' then 'Mobile Only' else b_fmc_status end as b_fmc_status,  
    case when num_row > 1 and e_fmc_status = 'Soft/Hard FMC' then 'Mobile Only' else e_fmc_status end as e_fmc_status,  
    case when num_row > 1 and b_fmctype = 'Soft/Hard FMC' then 'Mobile Only' else b_fmctype end as b_fmctype2,   
    case when num_row > 1 and e_fmctype = 'Soft/Hard FMC' then 'Mobile Only'
    else e_fmctype end as e_fmctype2    
FROM ( 
    SELECT
        *, 
        row_number() OVER (PARTITION BY fixed_account ORDER BY mobile_account desc) as num_row
    FROM "dg-sandbox"."cwc_fmc_feb2023"
)
WHERE 
    Final_Account like '%-%'
    and Fixed_Account = '995147450000'
ORDER BY Fixed_Account desc
-- LIMIT 10;
