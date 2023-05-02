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
    case when fixed_account is null then null else row_number() OVER (PARTITION BY fixed_account ORDER BY fixed_account, mobile_account desc) end as num_row_fixed
FROM Final_FixedBase f FULL OUTER JOIN Mobile_Base m
ON f.f_contactphone = m.Mobile_Phone and f.Fixed_Month = m.Mobile_Month
)

, repeated_both_panel as (
SELECT
    mobile_account, 
    fixed_account, 
    num_row_fixed, 
    case when mobile_account is null then null else row_number() OVER (PARTITION BY mobile_account ORDER BY mobile_account, fixed_account desc) end as num_row_mobile
FROM repeated_fixed_panel
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

--- ### Eliminating duplicated FMC accounts.
FROM (SELECT 
        case when num_row_fixed > 1 then null else fixed_account end as fixed_account, 
        case when num_row_mobile > 2 then null else mobile_account end as mobile_account
    FROM repeated_both_panel
    ) A
LEFT JOIN Final_FixedBase F
    ON A.fixed_account = F.fixed_account
LEFT JOIN Mobile_Base M
    ON A.mobile_account = M.mobile_account

--- ### Previous treatment (generates duplicates for FMC accounts)

-- Final_FixedBase f FULL OUTER JOIN Mobile_Base m
-- ON f.f_contactphone = m.Mobile_Phone and f.Fixed_Month = m.Mobile_Month
)

, CustomerBase_FMCFlags AS(
 
 SELECT t.*,
 CASE 
 WHEN (B_FMC_Status = 'Fixed Only' OR ((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC') AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL))) AND B_MixCode_Adj = '1P' THEN 'Fixed 1P'
 WHEN (B_FMC_Status = 'Fixed Only' OR ((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC') AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL)))AND B_MixCode_Adj = '2P' THEN 'Fixed 2P'
 WHEN (B_FMC_Status = 'Fixed Only' OR ((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC') AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL)) )AND B_MixCode_Adj = '3P' THEN 'Fixed 3P'
 WHEN (B_FMC_Status = 'Near FMC' OR B_FMC_Status = 'Soft/Hard FMC') AND (ActiveBOM = 0 OR ActiveBOM is null or B_NumRGUs= 0) AND Final_BOM_ActiveFlag = 1 then 'Mobile Only'
 WHEN (B_FMC_Status = 'Soft/Hard FMC' OR B_FMC_Status =  'Near FMC' OR  B_FMC_Status = 'Mobile Only') AND Final_BOM_ActiveFlag = 1 THEN B_FMC_Status
 END AS B_FMCType,
 CASE 
 WHEN (E_FMC_Status = 'Fixed Only' OR ((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC') AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR FinalMobileChurnFlag is not null)))  AND E_MixCode_Adj = '1P' and (FinalFixedChurnFlag is null or (FinalFixedChurnFlag = 'CST Churner' and ActiveEOM = 1)) THEN 'Fixed 1P'
 WHEN (E_FMC_Status = 'Fixed Only' OR ((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC') AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR FinalMobileChurnFlag is not null)))  AND E_MixCode_Adj = '2P' and (FinalFixedChurnFlag is null or (FinalFixedChurnFlag = 'CST Churner' and ActiveEOM = 1)) THEN 'Fixed 2P'
 WHEN (E_FMC_Status = 'Fixed Only' OR ((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC') AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR FinalMobileChurnFlag is not null)))  AND E_MixCode_Adj = '3P' and (FinalFixedChurnFlag is null or (FinalFixedChurnFlag = 'CST Churner' and ActiveEOM = 1)) THEN 'Fixed 3P'
 WHEN ((E_FMC_Status = 'Fixed Only' OR ((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC') AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL)))AND E_MixCode_Adj IS NULL AND (FinalFixedChurnFlag is not null AND FinalMobileChurnFlag is not null)) OR (FinalFixedChurnFlag is not null AND FinalMobileChurnFlag is not null)   THEN NULL
 WHEN (E_FMC_Status = 'Fixed Only' OR ((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC') AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL)))AND E_MixCode_Adj IS NULL AND FinalFixedChurnFlag IS NULL  THEN 'Fixed Gap Customer'
  WHEN ((E_FMC_Status = 'Mobile Only') OR ((E_FMC_Status = 'Near FMC' OR E_FMC_Status = 'Soft/Hard FMC') AND (ActiveEOM = 0 OR ActiveEOM is null or FixedChurnFlag = '1. Fixed Churner' OR E_NumRGUs = 0))) AND (FinalMobileChurnFlag is null)  then 'Mobile Only'
 WHEN E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status =  'Near FMC' THEN E_FMC_Status
 END AS E_FMCType,
 CASE WHEN ((FinalFixedChurnFlag is not null and FinalFixedChurnFlag <> 'CST Churner') and FinalMobileChurnFlag is not null) THEN 'Churner'
 WHEN (FinalFixedChurnFlag is not null and ((FinalFixedChurnFlag = 'CST Churner' and activeEOM = 0) or (FinalFixedChurnFlag <> 'CST Churner')) and FinalMobileChurnFlag is null) THEN 'Fixed Churner'
 WHEN ((FinalFixedChurnFlag is null OR FinalFixedChurnFlag = 'CST Churner')  AND FinalMobileChurnFlag is not null --AND Mobile_ActiveBOM = 1
 ) THEN 'Mobile Churner'
 ELSE 'Non Churner' END AS FinalChurnFlag
 FROM FullCustomerBase t
),

FullCustomerBase_FMCSegments AS(
SELECT DISTINCT f.*,
CASE WHEN ((B_FMCType = 'Soft/Hard FMC' OR B_FMCType = 'Near FMC') and (ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND B_MixCode_Adj = '1P') THEN 'P2'
WHEN ((B_FMCType = 'Soft/Hard FMC' OR B_FMCType = 'Near FMC') and (ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND B_MixCode_Adj = '2P') THEN 'P3'
WHEN ((B_FMCType = 'Soft/Hard FMC' OR B_FMCType = 'Near FMC')and (ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND B_MixCode_Adj = '3P') THEN 'P4'
WHEN ActiveBOM= 1 AND(Mobile_ActiveBOM= 0 OR Mobile_ActiveBOM IS NULL)  THEN 'P1_Fixed'
WHEN (ActiveBOM= 0 OR ActiveBOM IS NULL) AND Mobile_ActiveBOM= 1 THEN 'P1_Mobile'
END AS B_FMC_Segment,
CASE WHEN ((E_FMCType = 'Soft/Hard FMC' OR E_FMCType = 'Near FMC') and (ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND E_MixCode_Adj = '1P' AND FinalChurnFlag = 'Non Churner') THEN 'P2'
WHEN ((E_FMCType = 'Soft/Hard FMC' OR E_FMCType = 'Near FMC') and (ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND E_MixCode_Adj = '2P' AND FinalChurnFlag = 'Non Churner') THEN 'P3'
WHEN ((E_FMCType = 'Soft/Hard FMC'OR E_FMCType = 'Near FMC') and (ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND E_MixCode_Adj = '3P' AND FinalChurnFlag = 'Non Churner') THEN 'P4'
WHEN (E_FMC_Status = 'Fixed Only' and FinalChurnFlag = 'Fixed Churner') OR (E_FMC_Status = 'Mobile Only' and FinalChurnFlag = 'Mobile Churner')  OR FinalChurnFlag = 'Churner' THEN NULL
WHEN (E_FMCType = 'Soft/Hard FMC' OR E_FMCType = 'Near FMC') and ((ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND (E_MixCode_Adj IS NULL AND FinalChurnFlag = 'Non Churner')) OR (((ActiveEOM= 0 OR ActiveEOM IS NULL OR FinalChurnFlag = 'Fixed Churner' or FixedChurnFlag = '1. Fixed Churner') AND Mobile_ActiveEOM= 1)) THEN 'P1_Mobile'
WHEN (E_FMCType = 'Mobile Only' and Mobile_ActiveEOM = 1 and FinalChurnFlag <> 'Mobile Churner' and FinalChurnFlag <> 'Churner') THEN 'P1_Mobile'
WHEN (E_FMCType = 'Fixed Gap Customer') then E_FMCType
WHEN (ActiveEOM= 1 AND (Mobile_ActiveEOM= 0 OR Mobile_ActiveEOM IS NULL OR FinalChurnFlag = 'Mobile Churner') AND FixedChurnFlag <> '1. Fixed Churner')  THEN 'P1_Fixed'
WHEN (ActiveEOM = 1 AND FinalChurnFlag = 'Fixed Churner' AND (Mobile_ActiveEOM = 0 OR Mobile_ActiveEOM IS NULL)) OR (Mobile_ActiveEOM = 1 AND FinalChurnFlag = 'Mobile Churner' AND (ActiveEOM = 0 OR ActiveEOM IS NULL)) THEN NULL
END AS E_FMC_Segment,
CASE WHEN B_FMCType = 'Mobile Only' THEN 'Wireless'
WHEN  B_FMC_Status ='Fixed Only' or B_FMC_Status = 'Soft/Hard FMC' OR B_FMC_Status = 'Near FMC' THEN B_Tech_Type
END AS B_Final_Tech_Flag,
CASE WHEN (E_FMC_Status = 'Mobile Only' and FinalChurnFlag = 'Mobile Churner') 
OR (E_FMC_Status = 'Fixed Only' and FinalChurnFlag = 'Fixed Churner') OR
((E_FMC_Status = 'Soft/Hard FMC' OR E_FMC_Status ='Near FMC') AND FinalChurnFlag = 'Churner') THEN NULL
WHEN E_FMCType = 'Mobile Only' THEN 'Wireless' 
WHEN E_FMC_Status ='Fixed Only' or E_FMC_Status = 'Soft/Hard FMC'OR E_FMC_Status = 'Near FMC' THEN E_Tech_Type
END AS E_Final_Tech_Flag
FROM CustomerBase_FMCFlags f
),

FullCustomerBase_AllFlags AS(
SELECT DISTINCT f.*,
CASE WHEN (FinalChurnFlag = 'Churner') OR (FinalChurnFlag = 'Fixed Churner' and B_FMC_Segment = 'P1_Fixed' and E_FMC_Segment is null) OR (FinalChurnFlag = 'Mobile Churner' and B_FMC_Segment = 'P1_Mobile' and E_FMC_Segment is null) then 'Total Churner'
WHEN FinalChurnFlag = 'Non Churner' then null
ELSE 'Partial Churner' end as Partial_Total_ChurnFlag,
CASE WHEN (FinalChurnFlag = 'Churner' AND (FinalFixedChurnFlag = 'Voluntary' OR FinalFixedChurnFlag = 'Incomplete CST' OR FinalFixedChurnFlag = 'CST Churner') AND FinalMobileChurnFlag = 'Voluntary') OR (FinalChurnFlag = 'Fixed Churner' AND (FinalFixedChurnFlag = 'Voluntary' OR FinalFixedChurnFlag = 'Incomplete CST' OR FinalFixedChurnFlag = 'CST Churner')) OR (FinalChurnFlag = 'Mobile Churner' AND FinalMobileChurnFlag = 'Voluntary' OR FinalMobileChurnFlag = 'Incomplete CST') 
    THEN 'Voluntary'
WHEN (FinalChurnFlag = 'Churner' AND (FinalFixedChurnFlag = 'Involuntary' OR FinalFixedChurnFlag = 'Early Dx') AND (FinalMobileChurnFlag = 'Involuntary' OR FinalMobileChurnFlag = 'Early Dx')) OR (FinalChurnFlag = 'Fixed Churner' AND (FinalFixedChurnFlag = 'Involuntary' OR FinalFixedChurnFlag = 'Early Dx')) OR (FinalChurnFlag = 'Mobile Churner' AND (FinalMobileChurnFlag = 'Involuntary' OR FinalMobileChurnFlag = 'Early Dx')) THEN 'Involuntary'
WHEN FinalChurnFlag = 'Churner' AND (((FinalFixedChurnFlag = 'Involuntary' or FinalFixedChurnFlag = 'Early Dx') and (FinalMobileChurnFlag = 'Voluntary' or FinalMobileChurnFlag = 'Incomplete CST')) OR ((FinalFixedChurnFlag = 'Voluntary' or FinalFixedChurnFlag = 'Incomplete CST' or FinalFixedChurnFlag = 'CST Churner') and (FinalMobileChurnFlag = 'Involuntary' or FinalMobileChurnFlag = 'Early Dx'))) THEN 'Mixed'
END AS ChurnTypeFinalFlag,
CASE WHEN (FinalChurnFlag = 'Churner' AND (FinalFixedChurnFlag = 'Voluntary' AND FinalMobileChurnFlag = 'Voluntary')) OR (FinalChurnFlag = 'Fixed Churner' AND FinalFixedChurnFlag = 'Voluntary') OR (FinalChurnFlag = 'Mobile Churner' AND FinalMobileChurnFlag = 'Voluntary') THEN 'Voluntary'
WHEN (FinalChurnFlag = 'Churner' AND (FinalFixedChurnFlag = 'Incomplete CST' OR FinalFixedChurnFlag = 'CST Churner') AND FinalMobileChurnFlag = 'Incomplete CST') OR (FinalChurnFlag = 'Fixed Churner' AND (FinalFixedChurnFlag = 'Incomplete CST' OR FinalFixedChurnFlag = 'CST Churner')) OR (FinalChurnFlag = 'Mobile Churner' AND FinalMobileChurnFlag = 'Incomplete CST') THEN 'Incomplete CST'
WHEN FinalChurnFlag = 'Churner' AND (((FinalFixedChurnFlag = 'Incomplete CST' OR FinalFixedChurnFlag = 'CST Churner') AND FinalMobileChurnFlag = 'Voluntary') 
OR (FinalFixedChurnFlag = 'Voluntary' AND (FinalMobileChurnFlag = 'Incomplete CST' OR 
FinalMobileChurnFlag = 'CST Churner'))) THEN 'Mixed Voluntary/CST'
WHEN (FinalChurnFlag = 'Churner' AND FinalFixedChurnFlag = 'Early Dx' AND FinalMobileChurnFlag = 'Early Dx') OR (FinalChurnFlag = 'Fixed Churner' AND FinalFixedChurnFlag = 'Early Dx') OR (FinalChurnFlag = 'Mobile Churner' AND FinalMobileChurnFlag = 'Early Dx') THEN 'Early Dx'
WHEN (FinalChurnFlag = 'Churner' AND FinalFixedChurnFlag = 'Involuntary' AND FinalMobileChurnFlag = 'Involuntary') OR (FinalChurnFlag = 'Fixed Churner' AND FinalFixedChurnFlag = 'Involuntary') OR (FinalChurnFlag = 'Mobile Churner' AND FinalMobileChurnFlag = 'Involuntary') THEN 'Involuntary'
WHEN FinalChurnFlag = 'Churner' AND ((FinalFixedChurnFlag = 'Involuntary' AND FinalMobileChurnFlag = 'Early Dx') OR (FinalFixedChurnFlag = 'Early Dx' AND FinalMobileChurnFlag = 'Involuntary')) THEN 'Mixed Involuntary/Early Dx'
END AS ChurnSubtypeFinalFlag,
CASE WHEN (FinalChurnFlag = 'Churner' AND ChurnTenureSegment = '0.Early-tenure Churner' AND MobileChurnTenureSegment = 'Early-life') OR (FinalChurnFlag = 'Fixed Churner' AND ChurnTenureSegment = '0.Early-tenure Churner') OR (FinalChurnFlag = 'Mobile Churner' AND MobileChurnTenureSegment = 'Early-life') THEN 'Early tenure'
WHEN (FinalChurnFlag = 'Churner' AND ChurnTenureSegment = '1.Mid-tenure Churner' AND MobileChurnTenureSegment = 'Mid-life') OR (FinalChurnFlag = 'Fixed Churner' AND ChurnTenureSegment = '1.Mid-tenure Churner') OR (FinalChurnFlag = 'Mobile Churner' AND MobileChurnTenureSegment = 'Mid-life')  THEN 'Mid tenure'
WHEN (FinalChurnFlag = 'Churner' AND ChurnTenureSegment = '2.Late-tenure Churner' AND MobileChurnTenureSegment = 'Late-life') OR (FinalChurnFlag = 'Fixed Churner' AND ChurnTenureSegment = '2.Late-tenure Churner') OR (FinalChurnFlag = 'Mobile Churner' AND MobileChurnTenureSegment = 'Late-life')  THEN 'Late tenure'
WHEN FinalChurnFlag = 'Churner' AND ((ChurnTenureSegment = '0.Early-tenure Churner' AND (MobileChurnTenureSegment = 'Late-life' or MobileChurnTenureSegment = 'Mid-life')) OR ((ChurnTenureSegment = '2.Late-tenure Churner' or ChurnTenureSegment = '1.Mid-tenure Churner')  AND MobileChurnTenureSegment = 'Early-life')) THEN 'Early tenure'
END AS ChurnTenureFinalFlag
,CASE WHEN Fixed_RejoinerMonth = 1 AND (Mobile_RejoinerMonth IS NULL OR Mobile_RejoinerMonth = 0) and E_FMC_Segment = 'P1_Fixed' THEN 'Fixed Rejoiner'
WHEN Mobile_RejoinerMonth = 1  AND (Fixed_RejoinerMonth IS NULL OR Fixed_RejoinerMonth = 0 ) and E_FMC_Segment = 'P1_Mobile' THEN 'Mobile Rejoiner'
WHEN (Mobile_RejoinerMonth = 1 and Fixed_RejoinerMonth = 1) OR ((Fixed_RejoinerMonth = 1 OR Mobile_RejoinerMonth = 1) and  (E_FMCType = 'Soft/Hard FMC' OR E_FMCType = 'Near FMC')) THEN 'FMC Rejoiner'
END AS Rejoiner_FinalFlag
FROM FullCustomerBase_FMCSegments f
),

FullCustomersBase_Flags_Waterfall AS(

SELECT DISTINCT f.*,
CASE 
WHEN ((Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) or (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0)) AND (coalesce(B_TotalRGUs,0) > coalesce(E_TotalRGUs,0)) AND (MainMovement= '6.Null last day' OR MainMovement IS NULL) AND FinalChurnFlag = 'Non Churner' THEN 'Downsell-Fixed Customer Gap'
WHEN ((ActiveBOM = 1 and ActiveEOM = 1) AND (E_NumRGUs = 0) AND FixedChurnFlag <> '1. Fixed Churner') OR ((ActiveBOM = 1 and B_NumRGUs = 0)) OR (FixedChurnFlag = '1. Fixed Churner' and (ActiveBOM = 0 or ActiveBOM is null)) THEN 'Fixed Base Exceptions'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND ((MainMovement = '4.New Customer' AND MobileMovementFlag = '3.New Customer') OR (MainMovement = '4.New Customer' AND MobileMovementFlag IS NULL) OR (MainMovement IS NULL AND MobileMovementFlag = '3.New Customer'))  THEN 'Gross Ads'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND (MainMovement = '5.Come Back to Life' OR MobileMovementFlag = '4.Come Back to Life') AND (Rejoiner_FinalFlag IS NULL) THEN 'Gross Ads'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND (((Rejoiner_FinalFlag ='Fixed Rejoiner' OR Rejoiner_FinalFlag = 'Mobile Rejoiner') AND (E_FMCType = 'Soft/Hard FMC' OR E_FMCType = 'Near FMC'))  OR Rejoiner_FinalFlag = 'FMC Rejoiner') THEN 'FMC Rejoiner'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND (MainMovement = '5.Come Back to Life') AND (Rejoiner_FinalFlag = 'Fixed Rejoiner' AND E_FMC_Segment = 'P1_Fixed') THEN 'Fixed Rejoiner'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND (MobileMovementFlag = '4.Come Back to Life') AND (Rejoiner_FinalFlag = 'Mobile Rejoiner' AND E_FMC_Segment = 'P1_Mobile')  THEN 'Mobile Rejoiner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND  Rejoiner_FinalFlag IS NOT NULL then 'Semi-rejoiner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs < E_TotalRGUs) THEN 'Upsell'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs > E_TotalRGUs) THEN 'Downsell'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs = E_TotalRGUs) AND (B_TotalMRC = E_TotalMRC) THEN 'Maintain'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs = E_TotalRGUs) AND (B_TotalMRC < E_TotalMRC) THEN 'Upspin'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs = E_TotalRGUs) AND (B_TotalMRC > E_TotalMRC)  AND (E_TotalMRC <> 0 ) THEN 'Downspin'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0) AND (FinalChurnFlag <> 'Non Churner' AND ChurnTypeFinalFlag = 'Voluntary') THEN 'Voluntary Churners'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0) AND (FinalChurnFlag <> 'Non Churner' AND ChurnTypeFinalFlag = 'Involuntary') THEN 'Involuntary Churners'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0) AND (FinalChurnFlag <> 'Non Churner' AND ChurnTypeFinalFlag = 'Anticipated Involuntary') THEN 'Anticipated Involuntary Churners'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0) AND (FinalChurnFlag <> 'Non Churner' AND ChurnTypeFinalFlag = 'Mixed') THEN 'Mixed Churners'
END AS Waterfall_Flag
FROM FullCustomerBase_AllFlags f
)

,Final_Flags as(
select distinct f.*
,Case when MainMovement='3.Downsell' or E_MobileRGUs < B_MobileRGUs then 'Voluntary'
      when waterfall_flag='Downsell' and FinalChurnFlag <> 'Non Churner' then ChurnTypeFinalFlag
      when waterfall_flag='Downsell' and (mainmovement='6.Null last day' or (mobilemovementflag = '2.Loss' and MobileChurnFlag = '2. Mobile NonChurner')) then 'Undefined'
else null end as Downsell_Split
,case when waterfall_flag='Downspin' then 'Voluntary' else null end as Downspin_Split
from FullCustomersBase_Flags_Waterfall f
)

-- SELECT *
-- FROM Final_Flags
-- where month = date('2023-02-01')

SELECT 
    * 
FROM (SELECT *, row_number() OVER (PARTITION BY final_account ORDER BY f_contactphone desc) as num_row FROM Final_Flags) -- and A.f_contactphone = B.f_contactphone
WHERE 
    num_row = 1
    -- and (Fixed_Account = '995147450000'
    -- or Mobile_Account in ('293346970000', '297037560000', '159003400000'))
    -- and (mobile_account = '295191490000'
    -- or fixed_account in ('282239480000', '285239470000', '282239460000', '288316970000', '287239440000', '284239460000', '50278360', '288239480000', '284239450000', '286239470000', '283239480000', '287239430000', '322694590000', '326849880000'))
