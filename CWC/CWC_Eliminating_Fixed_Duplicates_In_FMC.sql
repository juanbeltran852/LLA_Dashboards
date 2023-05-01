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
    Fixed_Account, 
    case when Fixed_Account = f_test_account then 1 else null end as fmc_count
    -- count(distinct Final_Account), 
    -- count(distinct Fixed_Account), 
    -- count(distinct Mobile_Account), 
    -- case when  length(Fixed_Account) = 8 then substr()
FROM accounts_final
)

, accounts_tier as (
SELECT
    distinct Fixed_Account, 
    sum(fmc_count) as fmc_count
FROM accounts_count
GROUP BY 1
ORDER BY fmc_count desc
)

SELECT
    month, 
    case when num_row > 1 then mobile_account else final_account end as final_account, 
    case when num_row > 1 then mobile_activebom else final_bom_activeflag end as final_bom_activeflag,
    case when num_row > 1 then mobile_activeeom else final_eom_activeflag end as final_eom_activeflag,
    case 
        when num_row > 1 and mobile_activebom = 0 then null
        when num_row > 1 and mobile_activebom = 1 then 'Mobile Only' 
    else b_fmc_status end as b_fmc_status, 
    case 
        when num_row > 1 and mobile_activeeom = 0 then null
        when num_row > 1 and mobile_activeeom = 1 then 'Mobile Only' 
    else e_fmc_status end as e_fmc_status, 
    case when num_row > 1 then b_mobiletenuresegment else b_finaltenuresegment end as b_finaltenuresegment,
    case when num_row > 1 then e_mobiletenuresegment else e_finaltenuresegment end as e_finaltenuresegment,
    case when num_row > 1 then null else fixed_month end as fixed_month, 
    case when num_row > 1 then null else fixed_account end as fixed_account,
    case when num_row > 1 then null else activebom end as activebom,
    case when num_row > 1 then null else activeeom end as activeeom,
    case when num_row > 1 then null else b_fixedtenuresegment end as b_fixedtenuresegment,
    case when num_row > 1 then null else e_date end as e_date,
    case when num_row > 1 then null else e_tech_type end as e_tech_type,
    case when num_row > 1 then null else e_mixcode end as e_mixcode,
    case when num_row > 1 then null else e_mixcode_adj end as e_mixcode_adj,
    case when num_row > 1 then null else e_mixname end as e_mixname,
    case when num_row > 1 then null else e_mixname_adj end as e_mixname_adj,
    case when num_row > 1 then null else e_prodbbname end as e_prodbbname,
    case when num_row > 1 then null else e_prodtvname end as e_prodtvname,
    case when num_row > 1 then null else e_prodvoname end as e_prodvoname,
    case when num_row > 1 then null else bb_rgu_eom end as bb_rgu_eom,
    case when num_row > 1 then null else tv_rgu_eom end as tv_rgu_eom,
    case when num_row > 1 then null else vo_rgu_eom end as vo_rgu_eom,
    case when num_row > 1 then null else e_numrgus end as e_numrgus,
    case when num_row > 1 then null else e_bundlecode end as e_bundlecode,
    case when num_row > 1 then null else e_bundlename end as e_bundlename,
    case when num_row > 1 then null else e_mrc end as e_mrc,
    case when num_row > 1 then null else e_outstage end as e_outstage,
    case when num_row > 1 then null else e_mrcadj end as e_mrcadj,
    case when num_row > 1 then null else e_mrcbb end as e_mrcbb,
    case when num_row > 1 then null else e_mrctv end as e_mrctv,
    case when num_row > 1 then null else e_mrcvo end as e_mrcvo,
    case when num_row > 1 then null else e_avg_mrc end as e_avg_mrc,
    case when num_row > 1 then null else e_maxstart end as e_maxstart,
    case when num_row > 1 then null else e_tenuredays end as e_tenuredays,
    case when num_row > 1 then null else e_fixedtenuresegment end as e_fixedtenuresegment,
    case when num_row > 1 then null else mrcdiff end as mrcdiff,
    case when num_row > 1 then null else mainmovement end as mainmovement,
    case when num_row > 1 then null else spinmovement end as spinmovement,
    case when num_row > 1 then null else fixedchurnflag end as fixedchurnflag,
    case when num_row > 1 then null else fixedchurntypeflag end as fixedchurntypeflag,
    case when num_row > 1 then null else churntenuredays end as churntenuredays,
    case when num_row > 1 then null else churntenuresegment end as churntenuresegment,
    case when num_row > 1 then null else fixed_prmonth end as fixed_prmonth,
    case when num_row > 1 then null else fixed_rejoinermonth end as fixed_rejoinermonth,
    case when num_row > 1 then null else finalfixedchurnflag end as finalfixedchurnflag,
    case when num_row > 1 then null else f_contactphone end as f_contactphone,
    case when num_row > 1 then null else realfmc_flag end as realfmc_flag,
    mobile_month, 
    mobile_account,
    tenuredays,
    mobile_activebom,
    mobile_activeeom,
    mobile_b_date,
    mobile_b_tenuredays,
    b_mobile_maxstart,
    b_mobiletenuresegment,
    mobile_mrc_bom,
    b_avgmrc_mobile,
    b_mobilergus,
    b_mobilecustomertype,
    e_mobilecustomertype,
    mobile_e_date,
    mobile_e_tenuredays,
    e_mobile_maxstart,
    e_mobiletenuresegment,
    mobile_mrc_eom,
    e_avgmrc_mobile,
    e_mobilergus,
    mobilemovementflag,
    mobile_secondarymovementflag,
    mobile_mrc_diff,
    spinflag,
    mobilechurnflag,
    mobilechurntype,
    mobilechurntenuresegment,
    mobile_prmonth,
    mobile_rejoinermonth,
    finalmobilechurnflag,
    case when num_row > 1 then b_mobilergus else b_totalrgus end as b_totalrgus,
    case when num_row > 1 then e_mobilergus else e_totalrgus end as e_totalrgus,
    case when num_row > 1 then mobile_mrc_bom else b_totalmrc end as b_totalmrc,
    case when num_row > 1 then mobile_mrc_eom else e_totalmrc end as e_totalmrc,
    case when num_row > 1 and lower(b_fmctype) like '%fmc%' then 'Mobile Only' else b_fmctype end as b_fmctype, 
    case when num_row > 1 and lower(e_fmctype) like '%fmc%' then 'Mobile Only' else e_fmctype end as e_fmctype,
    case 
        when num_row > 1 and finalmobilechurnflag is not null then 'Mobile Churner'
        when num_row > 1 and finalmobilechurnflag is null then 'Non Churner' 
    else finalchurnflag end as finalchurnflag,
    case 
        when num_row > 1 and mobile_activebom = 1 then 'P1_Mobile' 
        when num_row > 1 and mobile_activebom = 0 then null
    else b_fmc_segment end as b_fmc_segment,
    case 
        when num_row > 1 and mobile_activeeom = 1 then 'P1_Mobile'
        when num_row > 1 and mobile_activeeom = 0 then null 
    else e_fmc_segment end as e_fmc_segment,
    case 
        when num_row > 1 and mobile_activebom = 0 then null
        when num_row > 1 and mobile_activebom = 1 then 'Wireless'
    else b_final_tech_flag end as b_final_tech_flag,
    case 
        when num_row > 1 and mobile_activeeom = 0 then null
        when num_row > 1 and mobile_activeeom = 1 then 'Wireless' 
    else e_final_tech_flag end as e_final_tech_flag,
    case 
        when num_row > 1 and b_mobilergus is not null and e_mobilergus is null then 'Total Churner'
        when num_row > 1 and b_mobilergus is not null and e_mobilergus is not null and b_mobilergus > e_mobilergus then 'Partial Chuners' 
    else partial_total_churnflag end as partial_total_churnflag,
    case when num_row > 1 then finalmobilechurnflag else churntypefinalflag end as churntypefinalflag,
    case when num_row > 1 then finalmobilechurnflag else churnsubtypefinalflag end as churnsubtypefinalflag,
    case 
        when num_row > 1 and mobilechurnflag = '1. Mobile Churner' and mobilechurntenuresegment = 'Early-life' then 'Early tenure'
        when num_row > 1 and mobilechurnflag = '1. Mobile Churner' and mobilechurntenuresegment = 'Mid-life' then 'Mid tenure'
        when num_row > 1 and mobilechurnflag = '1. Mobile Churner' and mobilechurntenuresegment = 'Late-life' then 'Late tenure'
    else churntenurefinalflag end as churntenurefinalflag,
    case 
        when num_row > 1 and mobile_rejoinermonth = 0 then null 
        when num_row > 1 and mobile_rejoinermonth = 1 then 'Mobile Rejoiner' 
    else rejoiner_finalflag end as rejoiner_finalflag,
    case 
        when num_row > 1 and mobile_activebom = 1 and b_mobilergus > e_mobilergus and finalmobilechurnflag is null then 'Downsell-Fixed Customer Gap'
        when num_row > 1 and (mobile_activebom = 0 and mobile_activeeom = 1) and ((mobilemovementflag = '3.New Customer') or (MobileMovementFlag = '4.Come Back to Life' and mobile_rejoinermonth = 0)) then 'Gross Ads'
        when num_row > 1 and (mobile_activebom = 0 and mobile_activeeom = 1) and (mobilemovementflag = '4.Come Back to Life') and (mobile_rejoinermonth = 1 and e_mobilergus = 1) then 'Mobile Rejoiner'
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 1) and (b_mobilergus < e_mobilergus) then 'Upsell'
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 1) and (b_mobilergus > e_mobilergus) then 'Downsell'
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 1) and (b_mobilergus > e_mobilergus) and (mobile_mrc_bom = mobile_mrc_eom) then 'Maintain'
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 1) and (b_mobilergus > e_mobilergus) and (mobile_mrc_bom < mobile_mrc_eom) then 'Upspin'
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 1) and (b_mobilergus > e_mobilergus) and (mobile_mrc_bom > mobile_mrc_eom) then 'Downspin'
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 0) and (finalmobilechurnflag = 'Voluntary') then 'Voluntary Churners'
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 0) and (finalmobilechurnflag = 'Involuntary') then 'Involuntary Churners'
    else waterfall_flag end as waterfall_flag, 
    case 
        when num_row > 1 and e_mobilergus < b_mobilergus then 'Voluntary'
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 1) and (b_mobilergus > e_mobilergus) and (finalmobilechurnflag is not null) then finalmobilechurnflag
        when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 1) and (b_mobilergus > e_mobilergus) and (mobilemovementflag = '2.Loss' and MobileChurnFlag = '2. Mobile NonChurner') then 'Undefined'
    else downsell_split end as downsell_split,
    case when num_row > 1 and (mobile_activebom = 1 and mobile_activeeom = 1) and (b_mobilergus > e_mobilergus) and (mobile_mrc_bom > mobile_mrc_eom) then 'Voluntary' else downspin_split end as downspin_split
FROM (SELECT *, row_number() OVER (PARTITION BY fixed_account ORDER BY mobile_account desc) as num_row FROM "dg-sandbox"."cwc_fmc_feb2023")
WHERE
    Fixed_Account in (SELECT Fixed_Account FROM accounts_tier WHERE fmc_count > 1)
--     and Fixed_Account = '995147450000'
ORDER BY Fixed_Account desc


-- SELECT
    -- *
    -- distinct waterfall_flag
-- FROM "dg-sandbox"."cwc_fmc_feb2023"
-- WHERE 
    -- Fixed_Account is null and Mobile_Account is not null
--     Fixed_Account in (SELECT Fixed_Account FROM accounts_tier WHERE fmc_count > 1)
    -- and e_mobilergus is null
-- ORDER BY Fixed_Account desc
