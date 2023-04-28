WITH

parameters as (SELECT date_trunc('month', date('2023-01-01')) as input_month)

, repeated_accounts as (
SELECT
    distinct fixed_account as fixed_account, 
    -- case when count(distinct mobile_account) > 0 then count(distinct mobile_account) else null end as num_mobile_account
    count(distinct mobile_account) as num_mobile_account
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
GROUP BY 1
-- HAVING 2 > 1
)


, relevant_accounts as (
SELECT
    month,
    final_account,
    fixed_account, 
    mobile_account, 
    b_fmc_status, 
    e_fmc_status, 
    b_fmctype, 
    e_fmctype, 
    finalchurnflag, 
    churntypefinalflag, 
    fixedchurnflag, 
    fixedchurntypeflag
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
WHERE
    month = (SELECT input_month FROM parameters)
    -- and final_account like '%-%'
    and fixed_account in (SELECT fixed_account FROM repeated_accounts WHERE num_mobile_account > 2)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
ORDER BY 3 desc
)

, flagscount_relevant_accounts as (
SELECT
    distinct fixed_account, 
    count(distinct b_fmc_status), 
    count(distinct e_fmc_status), 
    count(distinct b_fmctype), --- This
    count(distinct e_fmctype), -- This
    count(distinct finalchurnflag) -- This
FROM relevant_accounts
GROUP BY 1
ORDER BY 5 desc

)

, relevant_columns as (
SELECT
    -- *
    month, 
    final_account, 
    b_fmc_status, 
    e_fmc_status, 
    fixed_account, 
    mainmovement, 
    spinmovement, 
    fixedchurnflag, 
    fixedchurntypeflag, 
    f_contactphone,
    mobile_account,
    b_mobilecustomertype, 
    e_mobilecustomertype,
    spinflag, 
    mobilechurnflag, 
    mobilechurntype, 
    b_fmctype, 
    e_fmctype, 
    finalchurnflag, 
    waterfall_flag
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
WHERE
    date(month) = (SELECT input_month FROM parameters)
    -- and fixed_account = '995147450000'
    -- and fixed_account = '318257610000'
    and fixed_account = '965088240000'
    -- and mobile_account = '293346970000'
    -- and final_account not like '%-%'
    -- and fixed_account = mobile_account
)

SELECT * FROM relevant_columns ORDER BY 2 desc

-- SELECT * FROM relevant_accounts

-- SELECT * FROM repeated_accounts

-- SELECT * FROM flagscount_relevant_accounts
