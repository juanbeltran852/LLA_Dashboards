WITH

fmc_gitlab_test as (
SELECT
    *
FROM "dg-sandbox"."cwc_fmc_mar2023"
WHERE
    e_fmctype = 'Near FMC' -- or b_fmctype = 'Near FMC'
    -- and final_eom_activeflag = 1
)

, fmc_march_test as (
SELECT
    *
FROM "dg-sandbox"."cwc_fmc_mar2023_fmctrendtest"
WHERE
    e_fmctype = 'Near FMC'
    -- and final_eom_activeflag = 1
)

, lost_fmc_accounts as (
SELECT
    A.fixed_account,
    A.mobile_account,
    -- case when A.final_account is null and B.final_account is not null then B.final_account end as missed_in_gitlab,
    case when B.final_account is null and A.final_account is not null then A.final_account end as missed_in_march
FROM fmc_gitlab_test A
FULL OUTER JOIN fmc_march_test B
    ON A.final_account = B.final_account
)

SELECT
    distinct e_fmctype, 
    count(distinct final_account)
FROM "dg-sandbox"."cwc_fmc_mar2023_fmctrendtest"
WHERE
    mobile_account in (SELECT mobile_account FROM lost_fmc_accounts WHERE missed_in_march is not null)
GROUP BY 1



-- , lost_fmc_accounts_fixed as (
-- SELECT
--     A.e_fmctype, 
--     A.fixed_account, 
--     B.fixed_account,
--     B.e_fmctype as new_type,
--     case when B.fixed_account is null then A.fixed_account else null end as missed_fixed_flag
-- FROM fmc_gitlab_test A
-- LEFT JOIN fmc_march_test B
--     ON A.fixed_account = B.fixed_account 
-- )

-- , lost_fmc_accounts_mobile as (
-- SELECT
--     A.e_fmctype, 
--     A.mobile_account, 
--     B.mobile_account,
--     B.e_fmctype as new_type,
--     case when B.mobile_account is null then A.mobile_account else null end as missed_mobile_flag
-- FROM fmc_gitlab_test A
-- LEFT JOIN fmc_march_test B
--     ON A.mobile_account = B.mobile_account 
-- )

-- , lost_fmc_accounts as (
-- SELECT
--     distinct missed_accounts as missed_accounts
-- FROM (
--     SELECT
--         missed_fixed_flag as missed_accounts
--     FROM lost_fmc_accounts_fixed
--     UNION ALL (SELECT missed_mobile_flag FROM lost_fmc_accounts_mobile)
--     )
-- )

-- SELECT * FROM lost_fmc_accounts

-- SELECT 
--     A.*
-- FROM "dg-sandbox"."cwc_fmc_mar2023" A
-- LEFT JOIN lost_fmc_accounts B
--     ON (A.final_account = B.missed_accounts) or (A.fixed_account = B.missed_accounts) or (A.mobile_account = B.missed_accounts)
-- WHERE B.missed_accounts is not null
    
-- GROUP BY 1

-- SELECT distinct new_type, count(distinct missed_fixed_flag) FROM lost_fmc_accounts_fixed GROUP BY 1
-- WHERE
    -- e_fmctype = 'Near FMC'
    -- e_fmctype = 'Fixed 1P'
    -- final_account in ('50285054', '282122050000') --, '29621301', '301586710000')

-- SELECT
--     count(distinct final_account)
-- -- FROM "dg-sandbox"."cwc_fmc_mar2023" --- 363,363
-- FROM "dg-sandbox"."cwc_fmc_mar2023_fmctrendtest" --- 363,627
