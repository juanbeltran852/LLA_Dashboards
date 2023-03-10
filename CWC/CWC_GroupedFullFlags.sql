--- ########## GROUPED FULL FLAGS - PAULA MORENO (GITLAB) ##########

WITH 

fmc_full_flags_table as (
SELECT *
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
WHERE month=date(dt)
)

SELECT
    distinct Month, 
    Final_BOM_ActiveFlag, 
    Final_EOM_ActiveFlag, 
    B_FMC_Status, 
    E_FMC_Status, 
    B_FinalTenureSegment, 
    E_FinalTenureSegment,
    Fixed_Month, 
    ActiveBOM, 
    ActiveEOM, 
    B_Date, 
    B_Tech_Type, 
    B_MixCode, 
    B_MixCode_Adj, 
    B_MixName, 
    B_MixName_Adj, 
    B_ProdBBName, 
    B_ProdTVName, 
    B_ProdVoName, 
    B_NumRGUs,
    B_bundlecode, 
    B_bundlename, 
    B_FixedTenureSegment, 
    E_Date, 
    E_Tech_Type, 
    E_MixCode, 
    E_MixCode_Adj, 
    E_MixName, 
    E_MixName_Adj, 
    E_ProdBBName, 
    E_ProdTVName, 
    E_ProdVoName, 
    E_NumRGUs, 
    E_bundlecode, 
    E_bundlename, 
    E_FixedTenureSegment, 
    MainMovement, 
    SpinMovement, 
    FixedChurnFlag, 
    FixedChurnTypeFlag, 
    ChurnTenureSegment, 
    Fixed_PRMonth, 
    Fixed_RejoinerMonth, 
    realFMC_Flag, 
    Mobile_Month, 
    Mobile_ActiveBOM, 
    Mobile_ActiveEOM, 
    Mobile_B_Date, 
    B_MobileTenureSegment, 
    B_MobileRGUs, 
    B_MobileCustomerType, 
    E_MobileCustomerType, 
    Mobile_E_Date, 
    E_MobileTenureSegment, 
    E_MobileRGUs, 
    MobileMovementFlag, 
    Mobile_SecondaryMovementFlag, 
    SpinFlag, 
    MobileChurnFlag, 
    MobileChurnType, 
    MobileChurnTenureSegment, 
    Mobile_PRMonth, 
    Mobile_RejoinerMonth, 
    B_TotalRGUs, 
    E_TotalRGUs, 
    B_FMCType, 
    E_FMCType, 
    FinalChurnFlag, 
    B_FMC_Segment, 
    E_FMC_Segment, 
    B_Final_Tech_Flag, 
    E_Final_Tech_Flag, 
    Partial_Total_ChurnFlag, 
    ChurnTypeFinalFlag, 
    ChurnSubTypeFinalFLag, 
    ChurnTenureFinalFlag, 
    Rejoiner_FinalFlag, 
    Waterfall_Flag, 
    downsell_split, 
    downspin_split, 
    finalfixedchurnflag, 
    finalmobilechurnflag, 
    count(distinct Final_Account) as numaccounts, 
    count(distinct fixed_account) as numfixed, 
    count(distinct mobile_account) as nummobile,
    count(distinct bb_rgu_bom) as bb_rgus_bom, 
    count(distinct tv_rgu_bom) as tv_rgus_bom, 
    count(distinct vo_rgu_bom) as vo_rgus_bom, 
    count(distinct bb_rgu_eom) as bb_rgus_eom, 
    count(distinct tv_rgu_eom) as tv_rgus_eom, 
    count(distinct vo_rgu_eom) as vo_rgus_eom, 
    sum(B_MRC) as B_sum_MRC, 
    avg(cast(B_OutstAge as bigint)) as B_Avg_Overdue, 
    sum(cast(B_MRCAdj as double)) as B_AdjSum_MRC, 
    sum(B_MRCBB) as B_sum_MRCBB, 
    sum(B_MRCTV) as B_sum_MRCTV, 
    sum(B_MRCVO) as B_sum_MRCVO, 
    sum(B_Avg_MRC) as B_Sum_MonthAvgMRC, 
    avg(B_TenureDays) as B_AvgTenureDays, 
    sum(E_MRC) as E_SumMCR, 
    avg(cast(E_OutstAge as bigint)) as E_Avg_Overdue, 
    sum(cast(E_MRCAdj as double)) as E_AdjSum_MRC, 
    sum(E_MRCBB) as E_sum_MRCBB, 
    sum(E_MRCTV) as E_sum_MRCTV, 
    sum(E_MRCVO) as E_sum_MRCVO, 
    sum(E_Avg_MRC) as B_Sum_MonthAvgMRC, 
    avg(E_TenureDays) as E_AvgTenureDays, 
    avg(MRCDiff) as AvgMRCDiff, 
    avg(ChurnTenureDays) as Avg_churntenure, 
    avg(mobile_B_TenureDays) as avg_B_mobiletenure, 
    sum(mobile_MRC_BOM) as totalmobileMRCBOM, 
    sum(B_AvgMRC_Mobile) as sum_avgMRC_Mobile_B, 
    avg(mobile_E_TenureDays) as avg_E_mobiletenure, 
    sum(mobile_MRC_EOM) as totalmobileMRCEOM, 
    sum(E_AvgMRC_Mobile) as sum_avgMRC_Mobile_E, 
    avg(Mobile_MRC_Diff) as AvgMRCDiffMobile, 
    sum(B_TotalMRC) as B_totalMRC, 
    sum(E_TotalMRC) as E_TotalMRC
FROM fmc_full_flags_table
WHERE Month = date(dt)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40, 41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82
-- LIMIT 100
