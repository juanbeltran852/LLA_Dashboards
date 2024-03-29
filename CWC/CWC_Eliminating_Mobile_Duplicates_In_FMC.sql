WITH

accounts_final as (
SELECT Final_Account, Fixed_Account, Mobile_Account, 
    case 
        when length(Fixed_Account) = 8 and (Final_Account like '%-%') then substr(Final_Account, 1, 8)
        when length(Fixed_Account) = 12 and (Final_Account like '%-%') then substr(Final_Account, 1, 12)
        else null
    end as f_test_account, 
    case when (Final_Account like '%-%') then substr(Final_Account, -12) else null end as m_test_account
FROM "dg-sandbox"."cwc_fmc_feb2023"
where month = date('2023-02-01')
)

-- SELECT * FROM Final_Flags WHERE month = date('2023-02-01') LIMIT 100
, accounts_count as (
SELECT 
    Mobile_Account, 
    case when Mobile_Account = m_test_account then 1 else null end as fmc_count
    -- count(distinct Final_Account), 
    -- count(distinct Fixed_Account), 
    -- count(distinct Mobile_Account), 
    -- case when  length(Fixed_Account) = 8 then substr()
FROM accounts_final
)

, accounts_tier as (
SELECT
    distinct Mobile_Account, 
    sum(fmc_count) as fmc_count
FROM accounts_count
GROUP BY 1
ORDER BY fmc_count desc
)

SELECT
    month, 
    case when num_row > 2 then fixed_account else final_account end as final_account, 
    case when num_row > 2 then activebom else final_bom_activeflag end as final_bom_activeflag,
    case when num_row > 2 then activeeom else final_eom_activeflag end as final_eom_activeflag,
    case 
        when num_row > 2 and activebom = 0 then null
        when num_row > 2 and activebom = 1 then 'Fixed Only' 
    else b_fmc_status end as b_fmc_status, 
    case 
        when num_row > 2 and activeeom = 0 then null
        when num_row > 2 and activeeom = 1 then 'Fixed Only' 
    else e_fmc_status end as e_fmc_status, 
    case when num_row > 2 then b_fixedtenuresegment else b_finaltenuresegment end as b_finaltenuresegment,
    case when num_row > 2 then e_fixedtenuresegment else e_finaltenuresegment end as e_finaltenuresegment, 
    fixed_month,
    fixed_account,
    activebom,
    activeeom,
    b_date,
    b_tech_type,
    b_mixcode,
    b_mixcode_adj,
    b_mixname,
    b_mixname_adj,
    b_prodbbname,
    b_prodtvname,
    b_prodvoname,
    bb_rgu_bom,
    tv_rgu_bom,
    vo_rgu_bom,
    b_numrgus,
    b_bundlecode,
    b_bundlename,
    b_mrc,
    b_outstage,
    b_mrcadj,
    b_mrcbb,
    b_mrctv,
    b_mrcvo,
    b_avg_mrc,
    b_maxstart,
    b_tenuredays,
    b_fixedtenuresegment,
    e_date,
    e_tech_type,
    e_mixcode,
    e_mixcode_adj,
    e_mixname,
    e_mixname_adj,
    e_prodbbname,
    e_prodtvname,
    e_prodvoname,
    bb_rgu_eom,
    tv_rgu_eom,
    vo_rgu_eom,
    e_numrgus,
    e_bundlecode,
    e_bundlename,
    e_mrc,
    e_outstage,
    e_mrcadj,
    e_mrcbb,
    e_mrctv,
    e_mrcvo,
    e_avg_mrc,
    e_maxstart,
    e_tenuredays,
    e_fixedtenuresegment,
    mrcdiff,
    mainmovement,
    spinmovement,
    fixedchurnflag,
    fixedchurntypeflag,
    churntenuredays,
    churntenuresegment,
    fixed_prmonth,
    fixed_rejoinermonth,
    finalfixedchurnflag,
    f_contactphone,
    case when num_row > 2 then null else mobile_month end as mobile_month,
    case when num_row > 2 then null else mobile_account end as mobile_account,
    case when num_row > 2 then null else tenuredays end as tenuredays,
    case when num_row > 2 then null else mobile_activebom end as mobile_activebom,
    case when num_row > 2 then null else mobile_activeeom end as mobile_activeeom,
    case when num_row > 2 then null else mobile_b_date end as mobile_b_date,
    case when num_row > 2 then null else mobile_b_tenuredays end as mobile_b_tenuredays,
    case when num_row > 2 then null else b_mobile_maxstart end as b_mobile_maxstart,
    case when num_row > 2 then null else b_mobiletenuresegment end as b_mobiletenuresegment,
    case when num_row > 2 then null else mobile_mrc_bom end as mobile_mrc_bom,
    case when num_row > 2 then null else b_avgmrc_mobile end as b_avgmrc_mobile,
    case when num_row > 2 then null else b_mobilergus end as b_mobilergus,
    case when num_row > 2 then null else b_mobilecustomertype end as b_mobilecustomertype,
    case when num_row > 2 then null else e_mobilecustomertype end as e_mobilecustomertype,
    case when num_row > 2 then null else mobile_e_date end as mobile_e_date,
    case when num_row > 2 then null else mobile_e_tenuredays end as mobile_e_tenuredays,
    case when num_row > 2 then null else e_mobile_maxstart end as e_mobile_maxstart,
    case when num_row > 2 then null else e_mobiletenuresegment end as e_mobiletenuresegment,
    case when num_row > 2 then null else mobile_mrc_eom end as mobile_mrc_eom,
    case when num_row > 2 then null else e_avgmrc_mobile end as e_avgmrc_mobile,
    case when num_row > 2 then null else e_mobilergus end as e_mobilergus,
    case when num_row > 2 then null else mobilemovementflag end as mobilemovementflag,
    case when num_row > 2 then null else mobile_secondarymovementflag end as mobile_secondarymovementflag,
    case when num_row > 2 then null else mobile_mrc_diff end as mobile_mrc_diff,
    case when num_row > 2 then null else spinflag end as spinflag,
    case when num_row > 2 then null else mobilechurnflag end as mobilechurnflag,
    case when num_row > 2 then null else mobilechurntype end as mobilechurntype,
    case when num_row > 2 then null else mobilechurntenuresegment end as mobilechurntenuresegment,
    case when num_row > 2 then null else mobile_prmonth end as mobile_prmonth,
    case when num_row > 2 then null else mobile_rejoinermonth end as mobile_rejoinermonth,
    case when num_row > 2 then null else finalmobilechurnflag end as finalmobilechurnflag,
    case when num_row > 1 then b_numrgus else b_totalrgus end as b_totalrgus,
    case when num_row > 1 then e_numrgus else e_totalrgus end as e_totalrgus,
    case when num_row > 1 then b_mrc else b_totalmrc end as b_totalmrc,
    case when num_row > 1 then e_mrc else e_totalmrc end as e_totalmrc,
    case when num_row > 1 and lower(b_fmctype) like '%fmc%' then 'Fixed Only' else b_fmctype end as b_fmctype, 
    case when num_row > 1 and lower(e_fmctype) like '%fmc%' then 'Fixed Only' else e_fmctype end as e_fmctype,
    case 
        when num_row > 2 and finalfixedchurnflag is not null then 'Fixed Churner'
        when num_row > 2 and finalfixedchurnflag is null then 'Non Churner' 
    else finalchurnflag end as finalchurnflag,
    case 
        when num_row > 2 and activebom = 1 then 'P1_Fixed' 
        when num_row > 2 and activebom = 0 then null
    else b_fmc_segment end as b_fmc_segment,
    case 
        when num_row > 2 and activeeom = 1 then 'P1_Fixed'
        when num_row > 2 and activeeom = 0 then null 
    else e_fmc_segment end as e_fmc_segment,
    case 
        when num_row > 2 and activebom = 0 then null
        when num_row > 2 and activebom = 1 then b_tech_type
    else b_final_tech_flag end as b_final_tech_flag,
    case 
        when num_row > 2 and activeeom = 0 then null
        when num_row > 2 and activeeom = 1 then e_tech_type 
    else e_final_tech_flag end as e_final_tech_flag,
    case 
        when num_row > 2 and b_numrgus is not null and e_numrgus is null then 'Total Churner'
        when num_row > 2 and b_numrgus is not null and e_numrgus is not null and b_numrgus > e_numrgus then 'Partial Churner' 
    else partial_total_churnflag end as partial_total_churnflag,
    case when num_row > 2 then finalfixedchurnflag else churntypefinalflag end as churntypefinalflag,
    case when num_row > 2 then finalfixedchurnflag else churnsubtypefinalflag end as churnsubtypefinalflag,
    case 
        when num_row > 2 and fixedchurnflag = '1. Fixed Churner' and churntenuresegment = '0.Early-tenure Churner' then 'Early tenure'
        when num_row > 2 and fixedchurnflag = '1. Fixed Churner' and churntenuresegment = '1.Mid-tenure Churner' then 'Mid tenure'
        when num_row > 2 and fixedchurnflag = '1. Fixed Churner' and churntenuresegment = '2.Late-tenure Churner' then 'Late tenure'
    else churntenurefinalflag end as churntenurefinalflag,
    case 
        when num_row > 2 and fixed_rejoinermonth = 0 then null 
        when num_row > 2 and fixed_rejoinermonth = 1 then 'Fixed Rejoiner' 
    else rejoiner_finalflag end as rejoiner_finalflag,
    case 
        when num_row > 2 and activebom = 1 and b_numrgus > e_numrgus and (MainMovement = '6.Null last day' or MainMovement is null)  and finalfixedchurnflag is null then 'Downsell-Fixed Customer Gap'
        when num_row > 2 and ((activebom = 1 and activeeom = 1) and (e_numrgus = 0) and fixedchurnflag != '1. Fixed Churner') or ((activebom = 1 and b_numrgus = 0)) or (fixedchurnflag = '1. Fixed Churner' and (activebom = 0 or activebom is null)) then 'Fixed Base Exceptions'
        when num_row > 2 and (activebom = 0 and activeeom = 1) and ((mainmovement = '4.New Customer') or (mainmovement = '5.Come Back to Life' and fixed_rejoinermonth = 0)) then 'Gross Ads'
        when num_row > 2 and (activebom = 0 and activeeom = 1) and (mainmovement = '5.Come Back to Life') and (fixed_rejoinermonth = 1) then 'Fixed Rejoiner'
        when num_row > 2 and (activebom = 1 and activeeom = 1) and (b_numrgus < e_numrgus) then 'Upsell'
        when num_row > 2 and (activebom = 1 and activeeom = 1) and (b_numrgus > e_numrgus) then 'Downsell'
        when num_row > 2 and (activebom = 1 and activeeom = 1) and (b_numrgus > e_numrgus) and (b_mrc = e_mrc) then 'Maintain'
        when num_row > 2 and (activebom = 1 and activeeom = 1) and (b_numrgus > e_numrgus) and (b_mrc < e_mrc) then 'Upspin'
        when num_row > 2 and (activebom = 1 and activeeom = 1) and (b_numrgus > e_numrgus) and (b_mrc > e_mrc) then 'Downspin'
        when num_row > 2 and (activebom = 1 and activeeom = 0) and (finalfixedchurnflag = 'Voluntary') then 'Voluntary Churners'
        when num_row > 2 and (activebom = 1 and activeeom = 0) and (finalfixedchurnflag = 'Involuntary') then 'Involuntary Churners'
    else waterfall_flag end as waterfall_flag, 
    case 
        when num_row > 2 and mainmovement = '3.Downsell' then 'Voluntary'
        when num_row > 2 and (activebom = 1 and activeeom = 1) and (b_numrgus > e_numrgus) and (finalfixedchurnflag is not null) then finalfixedchurnflag
        when num_row > 2 and (activebom = 1 and activeeom = 1) and (b_numrgus > e_numrgus) and (mainmovement='6.Null last day') then 'Undefined'
    else downsell_split end as downsell_split,
    case when num_row > 2 and (activebom = 1 and activeeom = 1) and (b_numrgus > e_numrgus) and (b_mrc > e_mrc) then 'Voluntary' else downspin_split end as downspin_split
FROM (SELECT *, row_number() OVER (PARTITION BY mobile_account ORDER BY fixed_account desc) as num_row FROM "dg-sandbox"."cwc_fmc_feb2023")
WHERE
    Mobile_Account in (SELECT Mobile_Account FROM accounts_tier WHERE fmc_count > 1)
    -- and Mobile_Account = '295191490000'
ORDER BY Mobile_Account desc



-- SELECT
--     *
-- FROM "dg-sandbox"."cwc_fmc_feb2023"
-- WHERE Fixed_Account is not null and Mobile_Account is null
-- LIMIT 10
