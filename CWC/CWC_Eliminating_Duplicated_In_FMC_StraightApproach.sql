--CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cwc_con_san_KPIBaseTable_feb" AS
WITH 
Fixed_Base AS(
  SELECT * FROM "dg-sandbox"."cwc_fixed_feb2023"
)

,Mobile_Base AS(
  SELECT * FROM "dg-sandbox"."cwc_mobile_feb2023"
)

--###################################################### FMC Match############################################################################################
,FMC_Base as(
SELECT
date_trunc('MONTH',DATE( fix_dna.dt)) as fix_month
,fix_dna.act_acct_cd,fix_dna.bundle_code
,fix_dna.bundle_name,fix_dna.bundle_inst_date
,fix_dna.fmc_flag as fix_fmcflag,fix_dna.fmc_status
,fix_dna.fmc_start_date
,date_trunc('MONTH',DATE( mob_dna.dt)) as mob_month, mob_dna.account_id
,mob_dna.subscription_id,mob_dna.plan_code, mob_dna.phone_no
,mob_dna.plan_name,mob_dna.plan_activation_date,mob_dna.fmc_flag as mob_fmcflag
,mob_dna.fmc_household_id as mob_household
FROM "db-analytics-prod"."tbl_fixed_cwc" fix_dna
INNER join "db-analytics-prod"."tbl_postpaid_cwc" mob_dna on cast(mob_dna.org_id as int) = 338
and cast(mob_dna.run_id as int) = cast(to_char(cast(fix_dna.dt as date),'yyyymmdd') as int) 
and mob_dna.fmc_household_id = fix_dna.fmc_household_id
where fix_dna.org_cntry = 'Jamaica'
and mob_dna.fmc_flag = 'Y'
and fix_dna.dt = mob_dna.dt
)


,FixedFMCMatch AS(
 SELECT distinct Fixed_Month, Fixed_account as Account, phone_no
 FROM FMC_Base a
 INNER JOIN Fixed_Base  b
  ON a.act_acct_cd = b.Fixed_account
  and a.fix_month = b.Fixed_month
)

/*select fixed_month, count (distinct account)
from fixedfmcmatch
group by 1
order by 1*/

,MobileFMCMatch AS(
 
 SELECT distinct Mobile_Month, Act_acct_cd as Account, phone_no
 FROM FMC_Base a
 INNER JOIN Mobile_Base c
 --on a.account_id = c.mobile_account
 on a.phone_no = c.mobile_phone
 and a.mob_month = c.mobile_month
)

/*select mobile_month, count (distinct account)
from mobilefmcmatch
group by 1 
order by 1*/

,TotalMatch AS(
   SELECT DISTINCT f.Fixed_month as Month, m.account, m.Phone_no as FMCPhone
   FROM FixedFMCMatch f INNER JOIN MobileFMCMatch m on f.account = m.account
   and f.fixed_month = m.mobile_month
  --- GROUP BY m.account, fixed_month
)


,Fixed_MobileBaseMatch AS(
 Select DISTINCT Fixed_Month, Fixed_Account, f_contactphone1, f_contactphone2, f_contactphone3, sum(Match_Flag1) as Match_1, sum(Match_Flag2) as Match_2, sum(Match_Flag3) as Match_3
 FROM
 (Select f.*, 1 AS Match_Flag1, 0 AS Match_Flag2, 0 AS Match_Flag3
 FROM Fixed_Base f INNER JOIN Mobile_Base m ON f_contactphone1 = m.mobile_phone and f.fixed_month = m.mobile_month and f.B_DATE = m.Mobile_B_Date AND f.E_Date = m.Mobile_E_Date
 UNION ALL
 Select f.*, 0 AS Match_Flag1, 1 AS Match_Flag2, 0 AS Match_Flag3
 FROM Fixed_Base f INNER JOIN Mobile_Base m ON f_contactphone2 = m.mobile_phone and f.fixed_month = m.mobile_month and f.B_DATE = m.Mobile_B_Date AND f.E_Date = m.Mobile_E_Date
 UNION ALL
 Select f.*, 0 AS Match_Flag1, 0 AS Match_Flag2, 1 AS Match_Flag3
 FROM Fixed_Base f INNER JOIN Mobile_Base m ON f_contactphone3 = m.mobile_phone and f.fixed_month = m.mobile_month and f.B_DATE = m.Mobile_B_Date AND f.E_Date = m.Mobile_E_Date)
 GROUP BY fixed_month, Fixed_account, f_contactphone1, f_contactphone2, f_contactphone3 
)

,Fixed_Base_Phone_Adj AS(
 Select f.Fixed_Month, f.Fixed_Account, f.ActiveBOM,f.ActiveEOM,f.B_Date,f.B_Tech_Type, f.B_MixCode, f.B_MixCode_Adj, f.B_MixName, f.B_MixName_Adj, f.B_ProdBBName, f.B_ProdTVName, f.B_ProdVoName, f.BB_RGU_BOM, f.TV_RGU_BOM, f.VO_RGU_BOM,
 f.B_NumRGUs, f.B_bundlecode, f.B_bundlename, f.B_MRC , f.B_OutstAge, f.b_MRCAdj, f.B_MRCBB, f.B_MRCTV, f.B_MRCVO, f.B_Avg_MRC, f.B_MaxStart, f.B_TenureDays, 
 f.B_FixedTenureSegment, f.E_Date, f.E_Tech_Type, f.E_MixCode, f.E_MixCode_Adj, f.E_MixName, f.E_MixName_Adj, 
 f.E_ProdBBName, f.E_ProdTVName, f.E_ProdVoName, f.BB_RGU_EOM, f.TV_RGU_EOM, f.VO_RGU_EOM, f.E_NumRGUs, 
 f.E_bundlecode, f.E_bundlename, 
 f.E_MRC, f.E_OutstAge, f.E_MRCAdj, f.E_MRCBB, f.E_MRCTV,
 f.E_MRCVO, f.E_Avg_MRC, f.E_MaxStart, f.E_TenureDays, 
 f.E_FixedTenureSegment, f.MRCDiff, f.MainMovement, 
 f.SpinMovement,f.FixedChurnFlag, f.FixedChurnTypeFlag, f.ChurnTenureDays, 
 f.ChurnTenureSegment, f.Fixed_PRMonth, f.Fixed_RejoinerMonth, f.FinalFixedChurnFlag,
 --Select f.*, EXCEPT (f_contactphone1, f_contactphone2, f_contactphone3),
 CASE WHEN (Match_1 > 0 AND Match_2 > 0 AND Match_3 > 0) OR (Match_1 > 0 AND Match_2 > 0 AND Match_3 = 0) OR (Match_1 > 0 AND Match_2 = 0 AND Match_3 > 0) OR (Match_1 > 0 AND Match_2 = 0 AND Match_3 = 0) OR (Match_1 IS NULL AND Match_2 IS NULL AND Match_3 IS NULL)  THEN  f.f_contactphone1
 WHEN (Match_1 = 0 AND Match_2 > 0 AND Match_3 > 0) OR (Match_1 = 0 AND Match_2 > 0 AND Match_3 = 0)  THEN  f.f_contactphone2
 WHEN (Match_1 = 0 AND Match_2 = 0 AND Match_3 > 0 ) THEN f.f_contactphone3
 END AS f_contactphone
 FROM Fixed_Base f LEFT JOIN Fixed_MobileBaseMatch m ON f.fixed_month = m.fixed_month AND f.fixed_account = m.fixed_account
)

,Final_FixedBase as(
  Select f.Fixed_Month, f.Fixed_Account, f.ActiveBOM,f.ActiveEOM,f.B_Date,f.B_Tech_Type, f.B_MixCode, f.B_MixCode_Adj, f.B_MixName, f.B_MixName_Adj, f.B_ProdBBName, f.B_ProdTVName, f.B_ProdVoName,  f.BB_RGU_BOM, f.TV_RGU_BOM, f.VO_RGU_BOM,
 f.B_NumRGUs, f.B_bundlecode, f.B_bundlename, f.B_MRC , f.B_OutstAge, f.B_MRCAdj, f.B_MRCBB, f.B_MRCTV, f.B_MRCVO, f.B_Avg_MRC, f.B_MaxStart, f.B_TenureDays, 
 f.B_FixedTenureSegment, f.E_Date, f.E_Tech_Type, f.E_MixCode, f.E_MixCode_Adj, f.E_MixName, f.E_MixName_Adj, 
 f.E_ProdBBName, f.E_ProdTVName, f.E_ProdVoName, f.BB_RGU_EOM, f.TV_RGU_EOM, f.VO_RGU_EOM, f.E_NumRGUs, 
 f.E_bundlecode, f.E_bundlename, 
 f.E_MRC, f.E_OutstAge, f.E_MRCAdj, f.E_MRCBB, f.E_MRCTV,
 f.E_MRCVO, f.E_Avg_MRC, f.E_MaxStart, f.E_TenureDays, 
 f.E_FixedTenureSegment, f.MRCDiff, f.MainMovement, 
 f.SpinMovement,f.FixedChurnFlag, f.FixedChurnTypeFlag, f.ChurnTenureDays, 
 f.ChurnTenureSegment, f.Fixed_PRMonth, f.Fixed_RejoinerMonth, f.FinalFixedChurnFlag,
  --Select * Except(f_contactphone),
  CASE WHEN account is not null then FMCPhone 
  WHEN account is null then f_contactphone END AS f_contactphone,
  CASE WHEN account is not null then 'Real FMC'
  WHEN account is null then 'TBD' END AS RealFMC_Flag
  From Fixed_Base_Phone_Adj f left join totalmatch t ON cast(f.Fixed_account as bigint) = cast(t.account as bigint)
)

--###############################################JOIN Fixed--Mobile#############################################################################

, repeated_fixed_panel as (
SELECT
    fixed_account, 
    mobile_account, 
    row_number() OVER (PARTITION BY fixed_account ORDER BY fixed_account, mobile_account desc) as num_row_fixed
FROM Final_FixedBase f FULL OUTER JOIN Mobile_Base m
ON f.f_contactphone = m.Mobile_Phone and f.Fixed_Month = m.Mobile_Month
)

, repeated_mobile_panel as (
SELECT
    fixed_account,
    mobile_account,
    row_number() OVER (PARTITION BY mobile_account ORDER BY mobile_account, fixed_account desc) as num_row_mobile
FROM Final_FixedBase f FULL OUTER JOIN Mobile_Base m
ON f.f_contactphone = m.Mobile_Phone and f.Fixed_Month = m.Mobile_Month
)

,FullCustomerBase AS(
SELECT DISTINCT
CASE WHEN (A.Fixed_Account IS NOT NULL AND A.Mobile_Account IS NOT NULL) OR (A.Fixed_Account IS NOT NULL AND A.Mobile_Account IS NULL) THEN Fixed_Month
      WHEN (A.Fixed_Account IS NULL AND A.Mobile_Account IS NOT NULL) THEN Mobile_Month
  END AS Month,
CASE WHEN (A.Fixed_Account IS NOT NULL AND A.Mobile_Account IS NOT NULL) THEN concat(coalesce(A.fixed_account,''), '-', coalesce(A.mobile_account,''))
WHEN (A.Fixed_Account IS NOT NULL AND A.Mobile_Account IS NULL) THEN A.Fixed_Account
      WHEN (A.Fixed_Account IS NULL AND A.Mobile_Account IS NOT NULL) THEN A.Mobile_Account
  END AS Final_Account,
CASE WHEN (ActiveBOM =1 AND Mobile_ActiveBOM=1) or (ActiveBOM=1 AND (Mobile_ActiveBOM=0 or Mobile_ActiveBOM IS NULL)) or ((ActiveBOM=0 OR ActiveBOM IS NULL) AND Mobile_ActiveBOM=1) THEN 1
ELSE 0 END AS Final_BOM_ActiveFlag,
CASE WHEN (ActiveEOM =1 AND Mobile_ActiveEOM=1) or (ActiveEOM=1 AND (Mobile_ActiveEOM=0 or Mobile_ActiveEOM IS NULL)) or ((ActiveEOM=0 OR ActiveEOM IS NULL) AND Mobile_ActiveEOM=1) THEN 1
ELSE 0 END AS Final_EOM_ActiveFlag,
 CASE WHEN RealFMC_Flag = 'Real FMC' THEN 'Soft/Hard FMC'
 WHEN RealFMC_Flag = 'TBD' AND (A.Fixed_Account is not null and A.Mobile_Account is not null and ActiveBOM = 1 and Mobile_ActiveBOM = 1) THEN 'Near FMC'
 WHEN RealFMC_Flag = 'TBD' AND (A.Fixed_Account IS NOT NULL AND ActiveBOM=1 AND (Mobile_ActiveBOM = 0 OR Mobile_ActiveBOM IS NULL)) THEN 'Fixed Only'
 WHEN (RealFMC_Flag = 'TBD' AND (A.Mobile_Account IS NOT NULL AND Mobile_ActiveBOM=1 AND (ActiveBOM = 0 OR ActiveBOM IS NULL))) OR RealFMC_Flag IS NULL THEN 'Mobile Only'
 END AS B_FMC_Status,
 CASE WHEN RealFMC_Flag = 'Real FMC' THEN 'Soft/Hard FMC'
 WHEN RealFMC_Flag = 'TBD' AND (A.Fixed_Account is not null and A.Mobile_Account is not null and ActiveEOM = 1 and Mobile_ActiveEOM = 1) THEN 'Near FMC'
 WHEN RealFMC_Flag = 'TBD' AND (A.Fixed_Account IS NOT NULL AND ActiveEOM=1 AND (Mobile_ActiveEOM = 0 OR Mobile_ActiveEOM IS NULL)) THEN 'Fixed Only'
 WHEN (RealFMC_Flag = 'TBD' AND (A.Mobile_Account IS NOT NULL AND Mobile_ActiveEOM=1 AND (ActiveEOM = 0 OR ActiveEOM IS NULL))) OR RealFMC_Flag IS NULL THEN 'Mobile Only'
 END AS E_FMC_Status,
  CASE WHEN (B_FixedTenureSegment = 'Late-Tenure' and B_MobileTenureSegment = 'Late-Tenure') OR (B_FixedTenureSegment = 'Late-Tenure' and B_MobileTenureSegment IS NULL ) OR (B_FixedTenureSegment IS NULL and B_MobileTenureSegment = 'Late-Tenure') OR (B_FixedTenureSegment = 'Late-Tenure' and B_MobileTenureSegment in ('Mid-Tenure'))   Then 'Late-Tenure'
 WHEN (B_FixedTenureSegment = 'Mid-Tenure' and B_MobileTenureSegment = 'Mid-Tenure') OR (B_FixedTenureSegment = 'Mid-Tenure' and B_MobileTenureSegment IS NULL ) OR (B_FixedTenureSegment IS NULL and B_MobileTenureSegment = 'Mid-Tenure') OR (B_FixedTenureSegment = 'Mid-Tenure' and B_MobileTenureSegment in ('Late-Tenure')) Then 'Mid-Tenure'
WHEN (B_FixedTenureSegment = 'Early-Tenure' or B_MobileTenureSegment = 'Early-Tenure') THEN 'Early-Tenure'
END AS B_FinalTenureSegment,
 CASE WHEN (e_FixedTenureSegment = 'Late-Tenure' and e_MobileTenureSegment = 'Late-Tenure') OR (e_FixedTenureSegment = 'Late-Tenure' and e_MobileTenureSegment IS NULL ) OR (e_FixedTenureSegment IS NULL and e_MobileTenureSegment = 'Late-Tenure') OR (e_FixedTenureSegment = 'Late-Tenure' and e_MobileTenureSegment in ('Mid-Tenure'))   Then 'Late-Tenure'
 WHEN (e_FixedTenureSegment = 'Mid-Tenure' and e_MobileTenureSegment = 'Mid-Tenure') OR (e_FixedTenureSegment = 'Mid-Tenure' and e_MobileTenureSegment IS NULL ) OR (e_FixedTenureSegment IS NULL and e_MobileTenureSegment = 'Mid-Tenure') OR (e_FixedTenureSegment = 'Mid-Tenure' and e_MobileTenureSegment in ('Late-Tenure')) Then 'Mid-Tenure'
WHEN (e_FixedTenureSegment = 'Early-Tenure' or e_MobileTenureSegment = 'Early-Tenure') THEN 'Early-Tenure'
END AS e_FinalTenureSegment,
f.*, m.Mobile_Month, m.Mobile_Account,m.TenureDays,m.Mobile_ActiveBOM,m.Mobile_ActiveEOM,m.Mobile_B_Date, m.Mobile_B_TenureDays, m.B_Mobile_MaxStart,m.B_MobileTenureSegment,
m.Mobile_MRC_BOM, m.B_AvgMRC_Mobile
, m.B_MobileRGUs,
m.B_MobileCustomerType, m.E_MobileCustomerType,m.Mobile_E_Date, m.Mobile_E_TenureDays, m.E_Mobile_MaxStart, m.E_MobileTenureSegment, m.Mobile_MRC_EOM, m.E_AvgMRC_Mobile, 
m.E_MobileRGUs,
--count(distinct mobile_phone) as E_MobileRGUs,
m.MobileMovementFlag, m.Mobile_SecondaryMovementFlag, m.Mobile_MRC_Diff, m.SpinFlag, 
m.MobileChurnFlag, m.MobileChurnType, MobileChurnTenureSegment, m.Mobile_PRMonth, 
m.Mobile_RejoinerMonth, m.FinalMobileChurnFlag,

--f.*, m.* EXCEPT (mobile_phone),
(COALESCE(B_NumRGUs,0) + COALESCE(B_MobileRGUs,0)) as B_TotalRGUs, (COALESCE(E_NumRGUs,0) + COALESCE(E_MobileRGUs,0)) AS E_TotalRGUs,
cast((COALESCE(B_MRC,0) + COALESCE(Mobile_MRC_BOM, 0)) as integer) as B_TotalMRC, cast((COALESCE(E_MRC,0) + COALESCE(Mobile_MRC_EOM, 0))as integer) AS E_TotalMRC
-- FROM (SELECT 
--         case when A.num_row_fixed > 1 then null else A.fixed_account end as fixed_account, 
--         case when B.num_row_mobile > 2 then null else B.mobile_account end as mobile_account
--     FROM repeated_fixed_panel A
--     FULL OUTER JOIN repeated_mobile_panel B
--         ON ((A.fixed_account = B.fixed_account) or (A.mobile_account = B.mobile_account))
--     ) A
FROM (SELECT 
        case when num_row_fixed > 1 then null else fixed_account end as fixed_account, 
        mobile_account
    FROM repeated_fixed_panel
    ) A
LEFT JOIN Final_FixedBase F
    ON A.fixed_account = F.fixed_account
LEFT JOIN Mobile_Base M
    ON A.mobile_account = M.mobile_account

-- Final_FixedBase f FULL OUTER JOIN Mobile_Base m
-- ON f.f_contactphone = m.Mobile_Phone and f.Fixed_Month = m.Mobile_Month
)

-- SELECT 
--     fixed_account, 
--     mobile_account
-- FROM Final_FixedBase f 
-- FULL OUTER JOIN Mobile_Base m
--     ON f.f_contactphone = m.Mobile_Phone and f.Fixed_Month = m.Mobile_Month
-- -- FROM fullcustomerbase
-- WHERE 
--     fixed
--     Fixed_Account = '995147450000'

SELECT * FROM FullCustomerBase 
WHERE 
    Fixed_Account = '995147450000'
    or Mobile_Account in ('293346970000', '297037560000', '159003400000')
