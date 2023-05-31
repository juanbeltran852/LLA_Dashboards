-----------------------------------------------------------------------------------------
----------------------------- LCPR FMC TABLE - V2 ---------------------------------------
-----------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_fmc_feb2023_final" AS

WITH

parameters AS (
--> Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-02-01')) AS input_month
        ,85 as overdue_days
)

,fixed_table AS (
SELECT *
FROM "db_stage_dev"."lcpr_fixed_table_feb_apr14"
WHERE fix_s_dim_month = (SELECT input_month FROM parameters)
)

,mobile_table AS (
SELECT *
FROM "db_stage_dev"."lcpr_mob_feb2023_adj"
WHERE mob_s_dim_month = (SELECT input_month FROM parameters)
)

,convergency AS (
SELECT  *
        ,row_number() OVER (PARTITION BY fixed_account ORDER BY mobile_account desc) as row_fix
        ,row_number() OVER (PARTITION BY mobile_account ORDER BY fixed_account desc) as row_mob
FROM    (
        SELECT  fix_s_dim_month AS month
                ,fix_s_att_account AS fixed_account
                ,mob_s_att_account AS mobile_account
        FROM    (SELECT fix_s_dim_month,fix_s_att_account,fix_s_att_phone1 FROM fixed_table) A
                    INNER JOIN
                (SELECT mob_s_dim_month,mob_s_att_account FROM mobile_table) B
                    ON A.fix_s_dim_month = B.mob_s_dim_month AND A.fix_s_att_phone1 = B.mob_s_att_account
        UNION ALL
        SELECT  fix_s_dim_month AS month
                ,fix_s_att_account AS fixed_account
                ,mob_s_att_account AS mobile_account
        FROM    (SELECT fix_s_dim_month,fix_s_att_account,fix_s_att_phone2 FROM fixed_table) A
                    INNER JOIN
                (SELECT mob_s_dim_month,mob_s_att_account FROM mobile_table) B
                    ON A.fix_s_dim_month = B.mob_s_dim_month AND A.fix_s_att_phone2 = B.mob_s_att_account
        )
)

,BOM_prepaid_base AS (
SELECT  subsrptn_id AS prepaid_account
FROM "lcpr.stage.dev"."tbl_prepd_erc_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND ba_rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
)

,EOM_prepaid_base AS (
SELECT  subsrptn_id AS prepaid_account
FROM "lcpr.stage.dev"."tbl_prepd_erc_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters)
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND ba_rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
)

,FMC_base AS (
SELECT  IF(fix_s_dim_month IS NOT NULL,fix_s_dim_month,mob_s_dim_month) AS fmc_s_dim_month
        ,IF(fix_s_att_account IS NOT NULL AND mob_s_att_account IS NOT NULL,CONCAT(CAST(fix_s_att_account AS VARCHAR),' - ',CAST(mob_s_att_account AS VARCHAR(10))),IF(mob_s_att_account IS NULL,CAST(fix_s_att_account AS VARCHAR),CAST(mob_s_att_account AS VARCHAR(10)))) AS fmc_s_att_account
        ,IF(IF(fix_b_att_active IS NULL,0,fix_b_att_active) + IF(mob_b_att_active IS NULL,0,mob_b_att_active) >= 1,1,0) AS fmc_b_att_active
        ,IF(IF(fix_e_att_active IS NULL,0,fix_e_att_active) + IF(mob_e_att_active IS NULL,0,mob_e_att_active) >= 1,1,0) AS fmc_e_att_active
        ,fix_s_att_account
        ,fix_b_att_active
        ,fix_e_att_active
        ,fix_s_att_phone1
        ,fix_s_att_phone2
        ,fix_b_dim_date
        ,fix_b_mes_overdue
        ,fix_b_att_MaxStart
        ,fix_b_fla_tenure
        ,fix_b_mes_mrc
        ,fix_b_fla_tech
        ,fix_b_fla_fmc
        ,fix_b_mes_numRGUS
        ,fix_b_fla_MixNameAdj
        ,fix_b_fla_MixCodeAdj
        ,fix_b_fla_BB
        ,fix_b_fla_TV
        ,fix_b_fla_VO
        ,fix_b_att_bbCode
        ,fix_b_att_tvCode
        ,fix_b_att_voCode
        ,fix_b_fla_subsidized
        ,fix_b_att_BillCode
        ,fix_e_dim_date
        ,fix_e_mes_overdue
        ,fix_e_att_MaxStart
        ,fix_e_fla_tenure
        ,fix_e_mes_mrc
        ,fix_e_fla_tech
        ,fix_e_fla_fmc
        ,fix_e_mes_numRGUS
        ,fix_e_fla_MixNameAdj
        ,fix_e_fla_MixCodeAdj
        ,fix_e_fla_BB
        ,fix_e_fla_TV
        ,fix_e_fla_VO
        ,fix_e_att_bbCode
        ,fix_e_att_tvCode
        ,fix_e_att_voCode
        ,fix_e_fla_subsidized
        ,fix_e_att_BillCode
        ,fix_s_fla_MainMovement
        ,fix_s_fla_SpinMovement
        ,fix_s_fla_ChurnFlag
        ,fix_s_fla_ChurnType
        ,fix_s_fla_ChurnSubType
        ,fix_s_fla_Rejoiner
        ,mob_s_att_account
        ,mob_s_att_parentaccount
        ,mob_b_att_active
        ,mob_e_att_active
        ,mob_b_dim_date
        ,mob_b_mes_tenuredays
        ,mob_b_att_maxstart
        ,mob_b_fla_tenure
        ,mob_b_mes_mrc
        ,mob_b_mes_numrgus
        ,mob_e_dim_date
        ,mob_e_mes_tenuredays
        ,mob_e_att_maxstart
        ,mob_e_fla_tenure
        ,mob_e_mes_mrc
        ,mob_e_mes_numrgus
        ,mob_s_fla_mainmovement
        ,mob_s_mes_mrcdiff
        ,mob_s_fla_spinmovement
        ,mob_s_fla_churnflag
        ,mob_s_fla_churntype
        ,mob_s_fla_Rejoiner
        -- ,IF(mob_s_att_account IS NOT NULL /*AND mob_b_att_active = 1*/,row_number() IGNORE NULLS OVER (PARTITION BY mob_s_att_account ORDER BY fix_s_att_account desc),1) AS mob_s_att_duplicates
        ,row_number() OVER (PARTITION BY mob_s_att_account ORDER BY fix_s_att_account desc) AS mob_s_att_duplicates
        ,CASE   WHEN (fix_b_fla_tenure IS NOT NULL AND mob_b_fla_tenure IS NULL) THEN fix_b_fla_tenure
                WHEN (fix_b_fla_tenure = mob_b_fla_tenure) THEN fix_b_fla_tenure
                WHEN (mob_b_fla_tenure IS NOT NULL AND fix_b_fla_tenure IS NULL) THEN mob_b_fla_tenure
                WHEN (fix_b_fla_tenure <> mob_b_fla_tenure AND (fix_b_fla_tenure = 'Early-Tenure' or mob_b_fla_tenure = 'Early-Tenure' )) THEN 'Early-Tenure'
                WHEN (fix_b_fla_tenure <> mob_b_fla_tenure AND (fix_b_fla_tenure = 'Mid-Tenure' or mob_b_fla_tenure = 'Mid-Tenure' )) THEN 'Mid-Tenure'
                    END AS fmc_b_fla_tenure
        ,CASE   WHEN (fix_e_fla_tenure IS NOT NULL AND mob_e_fla_tenure IS NULL) THEN fix_e_fla_tenure
                WHEN (fix_e_fla_tenure = mob_e_fla_tenure) THEN fix_e_fla_tenure
                WHEN (mob_e_fla_tenure IS NOT NULL AND fix_e_fla_tenure IS NULL) THEN mob_e_fla_tenure
                WHEN (fix_e_fla_tenure <> mob_e_fla_tenure AND (fix_e_fla_tenure = 'Early-Tenure'  or mob_e_fla_tenure = 'Early-Tenure' )) THEN 'Early-Tenure'
                WHEN (fix_e_fla_tenure <> mob_e_fla_tenure AND (fix_e_fla_tenure = 'Mid-Tenure'  or mob_e_fla_tenure = 'Mid-Tenure' )) THEN 'Mid-Tenure'
                    END AS fmc_e_fla_tenure
        ,IF(fix_b_mes_numRGUS IS NULL,0,fix_b_mes_numRGUS) + IF(mob_b_mes_numRGUS IS NULL,0,mob_b_mes_numRGUS) AS fmc_b_mes_numRGUS
        ,IF(fix_e_mes_numRGUS IS NULL,0,fix_e_mes_numRGUS) + IF(mob_e_mes_numRGUS IS NULL,0,mob_e_mes_numRGUS) AS fmc_e_mes_numRGUS
        ,IF(fix_b_mes_mrc IS NULL,0,fix_b_mes_mrc) + IF(mob_b_mes_mrc IS NULL,0,mob_b_mes_mrc) AS fmc_b_mes_mrc
        ,IF(fix_e_mes_mrc IS NULL,0,fix_e_mes_mrc) + IF(mob_e_mes_mrc IS NULL,0,mob_e_mes_mrc) AS fmc_e_mes_mrc
        ,CASE   WHEN fix_b_att_active = 1 AND IF(mob_b_att_active IS NULL,0,mob_b_att_active) = 0 THEN IF((fix_s_att_phone1 IN (SELECT DISTINCT prepaid_account FROM BOM_prepaid_base) OR fix_s_att_phone2 IN (SELECT DISTINCT prepaid_account FROM BOM_prepaid_base)),IF(fix_b_att_BillCode IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Prepaid Real FMC','Prepaid Near FMC'),'Fixed Only')
                WHEN IF(fix_b_att_active IS NULL,0,fix_b_att_active) = 0 AND mob_b_att_active = 1 THEN 'Mobile Only'
                WHEN fix_b_att_active + mob_b_att_active = 2 THEN IF((fix_s_att_phone1 IN (SELECT DISTINCT prepaid_account FROM BOM_prepaid_base) OR fix_s_att_phone2 IN (SELECT DISTINCT prepaid_account FROM BOM_prepaid_base)),IF(fix_b_att_BillCode IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Real FMC','Near FMC'),IF(fix_b_att_BillCode IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Postpaid Real FMC','Postpaid Near FMC'))
                    ELSE NULL END AS fmc_b_fla_fmc
        ,CASE   WHEN fix_e_att_active = 1 AND IF(mob_e_att_active IS NULL,0,mob_e_att_active) = 0 THEN IF((fix_s_att_phone1 IN (SELECT DISTINCT prepaid_account FROM EOM_prepaid_base) OR fix_s_att_phone2 IN (SELECT DISTINCT prepaid_account FROM EOM_prepaid_base)),IF(fix_e_att_BillCode IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Prepaid Real FMC','Prepaid Near FMC'),'Fixed Only')
                WHEN IF(fix_e_att_active IS NULL,0,fix_e_att_active) = 0 AND mob_e_att_active = 1 THEN 'Mobile Only'
                WHEN fix_e_att_active + mob_e_att_active = 2 THEN IF((fix_s_att_phone1 IN (SELECT DISTINCT prepaid_account FROM EOM_prepaid_base) OR fix_s_att_phone2 IN (SELECT DISTINCT prepaid_account FROM EOM_prepaid_base)),IF(fix_e_att_BillCode IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Real FMC','Near FMC'),IF(fix_e_att_BillCode IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Postpaid Real FMC','Postpaid Near FMC'))
                    ELSE NULL END AS fmc_e_fla_fmc
FROM    (
        SELECT A.*, B.mobile_account
        FROM fixed_table A LEFT JOIN (SELECT * FROM convergency WHERE row_fix = 1 AND row_mob <= 2) B
            ON A.fix_s_att_account = B.fixed_account AND A.fix_s_dim_month = B.month
        WHERE fix_s_att_account IS NOT NULL
        ) C FULL OUTER JOIN mobile_table D
    ON C.mobile_account = D.mob_s_att_account AND C.fix_s_dim_month = D.mob_s_dim_month
)

,FMC_base_adj AS (
SELECT  *
        ,IF(fmc_b_fla_fmc = 'Fixed Only',CONCAT(fix_b_fla_MixCodeAdj,' Fixed'),IF(fmc_b_fla_fmc = 'Mobile Only','P1 Mobile',CONCAT(CAST((CAST(SUBSTR(fix_b_fla_MixCodeAdj,1,1) AS int) + 1) AS VARCHAR),'P ',fmc_b_fla_fmc))) AS fmc_b_fla_FMCsegment
        ,IF(fmc_e_fla_fmc = 'Fixed Only',CONCAT(fix_e_fla_MixCodeAdj,' Fixed'),IF(fmc_e_fla_fmc = 'Mobile Only','P1 Mobile',CONCAT(CAST((CAST(SUBSTR(fix_e_fla_MixCodeAdj,1,1) AS int) + 1) AS VARCHAR),'P ',fmc_e_fla_fmc))) AS fmc_e_fla_FMCsegment
        ,IF(fmc_b_fla_fmc = 'Mobile Only','WIRELESS',fix_b_fla_tech) AS fmc_b_fla_tech
        ,IF(fmc_e_fla_fmc = 'Mobile Only','WIRELESS',fix_e_fla_tech) AS fmc_e_fla_tech
        ,CASE   WHEN fix_s_fla_Rejoiner = 1 AND mob_s_fla_Rejoiner = 1 THEN '1. Full Rejoiner'
                WHEN fix_s_fla_Rejoiner = 1 AND (mob_s_fla_Rejoiner = 0 OR mob_s_fla_Rejoiner IS NULL) THEN '2. Fixed Rejoiner'
                WHEN mob_s_fla_Rejoiner = 1 AND (fix_s_fla_Rejoiner = 0 OR fix_s_fla_Rejoiner IS NULL) THEN '3. Mobile Rejoiner'
                    ELSE NULL END AS fmc_s_fla_Rejoiner
        ,CASE   WHEN (fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_s_fla_ChurnType <> '3. Fixed Transfer' AND mob_s_fla_ChurnFlag = '1. Mobile Churner') THEN 'Churner'
                WHEN (fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_s_fla_ChurnType <> '3. Fixed Transfer' AND (mob_s_fla_ChurnFlag =  '2. Mobile NonChurner' OR mob_s_fla_ChurnFlag IS NULL) ) THEN 'Fixed Churner'
                WHEN (fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_s_fla_ChurnType = '3. Fixed Transfer' AND mob_s_fla_ChurnFlag =  '1. Mobile Churner') THEN 'Mobile Churner'
                WHEN ((fix_s_fla_ChurnFlag = '2. Fixed NonChurner' OR fix_s_fla_ChurnFlag IS NULL) AND mob_s_fla_ChurnFlag =  '1. Mobile Churner') THEN 'Mobile Churner'
                    ELSE 'Non Churner' END AS fmc_s_fla_ChurnFlag
        ,CASE   WHEN (fix_s_fla_ChurnType = '1. Fixed Voluntary Churner' AND mob_s_fla_ChurnType = '1. Mobile Voluntary Churner')
                    OR (fix_s_fla_ChurnType = '1. Fixed Voluntary Churner' AND mob_s_fla_ChurnType IS NULL) 
                    OR (fix_s_fla_ChurnType IS NULL AND mob_s_fla_ChurnType = '1. Mobile Voluntary Churner')
                    OR (fix_s_fla_ChurnType = '3. Fixed Transfer' AND mob_s_fla_ChurnType = '1. Mobile Voluntary Churner')
                        THEN 'Voluntary Churner'
                WHEN (fix_s_fla_ChurnType = '2. Fixed Involuntary Churner' AND mob_s_fla_ChurnType = '2. Mobile Involuntary Churner')
                    OR (fix_s_fla_ChurnType = '2. Fixed Involuntary Churner' AND mob_s_fla_ChurnType IS NULL) 
                    OR (fix_s_fla_ChurnType IS NULL AND mob_s_fla_ChurnType = '2. Mobile Involuntary Churner') 
                    OR (fix_s_fla_ChurnType = '3. Fixed Transfer' AND mob_s_fla_ChurnType = '2. Mobile Involuntary Churner')
                        THEN 'Involuntary Churner'
                WHEN (fix_s_fla_ChurnType = '1. Fixed Voluntary Churner' AND mob_s_fla_ChurnType = '2. Mobile Involuntary Churner') 
                    OR (fix_s_fla_ChurnType = '2. Fixed Involuntary Churner' AND mob_s_fla_ChurnType = '1. Mobile Voluntary Churner')
                        THEN 'Mixed Churner'
                WHEN fix_s_fla_ChurnType = '3. Fixed 0P Churner' AND mob_s_fla_ChurnType IS NULL THEN '0P Churner'
                WHEN fix_s_fla_ChurnType = '3. Fixed Transfer' AND mob_s_fla_ChurnType IS NULL THEN 'Fixed Transfer'
                        ELSE 'Non Churner' END AS fmc_s_fla_ChurnType
FROM FMC_base
)

,final_flags AS (
SELECT  fmc_s_dim_month
        ,fmc_s_att_account
        ,fmc_b_att_active
        ,fmc_e_att_active
        ,fix_s_att_account
        ,fix_b_att_active
        ,fix_e_att_active
        ,fix_s_att_phone1
        ,fix_s_att_phone2
        ,fix_b_dim_date
        ,fix_b_mes_overdue
        ,fix_b_att_MaxStart
        ,fix_b_fla_tenure
        ,fix_b_mes_mrc
        ,fix_b_fla_tech
        ,fix_b_fla_fmc
        ,fix_b_mes_numRGUS
        ,fix_b_fla_MixNameAdj
        ,fix_b_fla_MixCodeAdj
        ,fix_b_fla_BB
        ,fix_b_fla_TV
        ,fix_b_fla_VO
        ,fix_b_att_bbCode
        ,fix_b_att_tvCode
        ,fix_b_att_voCode
        ,fix_b_fla_subsidized
        ,fix_b_att_BillCode
        ,fix_e_dim_date
        ,fix_e_mes_overdue
        ,fix_e_att_MaxStart
        ,fix_e_fla_tenure
        ,fix_e_mes_mrc
        ,fix_e_fla_tech
        ,fix_e_fla_fmc
        ,fix_e_mes_numRGUS
        ,fix_e_fla_MixNameAdj
        ,fix_e_fla_MixCodeAdj
        ,fix_e_fla_BB
        ,fix_e_fla_TV
        ,fix_e_fla_VO
        ,fix_e_att_bbCode
        ,fix_e_att_tvCode
        ,fix_e_att_voCode
        ,fix_e_fla_subsidized
        ,fix_e_att_BillCode
        ,fix_s_fla_MainMovement
        ,fix_s_fla_SpinMovement
        ,fix_s_fla_ChurnFlag
        ,fix_s_fla_ChurnType
        ,fix_s_fla_ChurnSubType
        ,fix_s_fla_Rejoiner
        ,mob_s_att_account
        ,mob_s_att_parentaccount
        ,mob_b_att_active
        ,mob_e_att_active
        ,mob_b_dim_date
        ,mob_b_mes_tenuredays
        ,mob_b_att_maxstart
        ,mob_b_fla_tenure
        ,mob_b_mes_mrc
        ,mob_b_mes_numrgus
        ,mob_e_dim_date
        ,mob_e_mes_tenuredays
        ,mob_e_att_maxstart
        ,mob_e_fla_tenure
        ,mob_e_mes_mrc
        ,mob_e_mes_numrgus
        ,mob_s_fla_mainmovement
        ,mob_s_mes_mrcdiff
        ,mob_s_fla_spinmovement
        ,mob_s_fla_churnflag
        ,mob_s_fla_churntype
        ,mob_s_fla_Rejoiner
        ,mob_s_att_duplicates
        ,fmc_b_fla_tenure
        ,fmc_e_fla_tenure
        ,fmc_b_mes_numrgus
        ,fmc_e_mes_numrgus
        ,fmc_b_mes_mrc
        ,fmc_e_mes_mrc
        ,fmc_b_fla_fmc
        ,fmc_e_fla_fmc
        ,fmc_b_fla_FMCsegment
        ,fmc_e_fla_FMCsegment
        ,fmc_b_fla_tech
        ,fmc_e_fla_tech
        ,CASE   WHEN (fmc_s_fla_Rejoiner = '2. Fixed Rejoiner' OR fmc_s_fla_Rejoiner = '3. Mobile Rejoiner') AND fmc_e_fla_FMCsegment IN ('2P FMC','3P FMC','4P FMC') THEN '1. FMC Rejoiner'
                    ELSE fmc_s_fla_Rejoiner END AS fmc_s_fla_Rejoiner
        ,fmc_s_fla_ChurnFlag
        ,fmc_s_fla_ChurnType
        ,CASE   WHEN (fmc_s_fla_ChurnFlag = 'Churner') OR (fmc_s_fla_ChurnFlag = 'Fixed Churner' /*AND fmc_b_fla_FMCsegment = 'P1 Fixed'*/ and fmc_e_fla_FMCsegment is null and fmc_s_fla_ChurnType <> 'Fixed Transfer') OR (fmc_s_fla_ChurnFlag = 'Mobile Churner' and fmc_b_fla_FMCsegment = 'P1 Mobile' and fmc_e_fla_FMCsegment is null and fmc_s_fla_ChurnType <> 'Fixed Transfer') THEN 'Total Churner'
                WHEN fmc_s_fla_ChurnFlag = 'Non Churner' THEN NULL
                    ELSE 'Partial Churner' END AS fmc_s_fla_PartialTotalChurn
        ,CASE   WHEN (fmc_b_att_active = 0 AND fmc_e_att_active = 1) AND ((fix_s_fla_MainMovement IN ('4.New Customer','8.Rejoiner-GrossAdd Gap') AND mob_s_fla_MainMovement = '4.New Customer') OR (fix_s_fla_MainMovement IN ('4.New Customer','8.Rejoiner-GrossAdd Gap') AND mob_s_fla_MainMovement IS NULL) OR (fix_s_fla_MainMovement IS NULL AND mob_s_fla_MainMovement = '4.New Customer')) THEN 'Gross Adds'
                WHEN (fix_b_att_active = 0 and fix_e_att_active = 1) AND fix_s_fla_MainMovement IS NULL AND fix_e_att_MaxStart IS NULL THEN 'GrossAdds-Fixed Customer Gap'
                WHEN (fmc_b_att_active = 0 and fmc_e_att_active = 1) AND (fix_s_fla_MainMovement = '5.Come Back to Life' OR mob_s_fla_MainMovement = '5.Come Back to Life') AND fmc_s_fla_ChurnFlag <> 'Non Churner' THEN 'ComeBackToLife-Fixed Customer Gap'
                WHEN fmc_b_att_active = 0 AND fmc_e_att_active = 1 AND fmc_s_fla_Rejoiner = '1. Full Rejoiner' AND fmc_e_fla_FMCsegment IN ('2P FMC','3P FMC','4P FMC') THEN '5.1. FMC Rejoiner'
                WHEN fmc_b_att_active = 0 AND fmc_e_att_active = 1 AND fmc_s_fla_Rejoiner = '2. Fixed Rejoiner' AND fmc_e_fla_FMCsegment NOT IN ('2P FMC','3P FMC','4P FMC') THEN '5.2. Fixed Rejoiner'
                WHEN fmc_b_att_active = 0 AND fmc_e_att_active = 1 AND fmc_s_fla_Rejoiner = '3. Mobile Rejoiner' AND fmc_e_fla_FMCsegment NOT IN ('2P FMC','3P FMC','4P FMC') THEN '5.3. Mobile Rejoiner'
                WHEN fmc_b_att_active = 1 AND fmc_e_att_active = 1 AND fmc_s_fla_Rejoiner IS NOT NULL THEN '5.4 Near Rejoiner'
                WHEN (fmc_b_att_active = 0 and fmc_e_att_active = 1) AND (fix_s_fla_MainMovement = '5.Come Back to Life' OR mob_s_fla_MainMovement = '5.Come Back to Life') AND fmc_s_fla_Rejoiner IS NULL THEN 'Gross Adds'
                WHEN (fmc_b_att_active = 0 and fmc_e_att_active = 1) AND fmc_s_fla_ChurnType = 'Non Churner' AND fix_s_fla_MainMovement = '7.Transfer Adds' THEN 'Fixed Transfer Adds'
                WHEN fmc_s_fla_ChurnType = 'Fixed Transfer' THEN 'Fixed Transfer Churn'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 1) AND (fmc_b_mes_numRGUS < fmc_e_mes_numRGUS) THEN 'Upsell'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 1) AND (fmc_b_mes_numRGUS > fmc_e_mes_numRGUS) THEN 'Downsell'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 1) AND (fmc_b_mes_numRGUS = fmc_e_mes_numRGUS) AND (fmc_b_mes_mrc = fmc_e_mes_mrc) THEN 'Maintain'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 1) AND (fmc_b_mes_numRGUS = fmc_e_mes_numRGUS) AND (fmc_b_mes_mrc < fmc_e_mes_mrc) THEN 'Upspin'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 1) AND (fmc_b_mes_numRGUS = fmc_e_mes_numRGUS) AND (fmc_b_mes_mrc > fmc_e_mes_mrc) THEN 'Downspin'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 0) AND (fmc_s_fla_ChurnFlag <> 'Non Churner' AND fmc_s_fla_ChurnType = 'Voluntary Churner') THEN 'Voluntary Churner'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 0) AND (fmc_s_fla_ChurnFlag <> 'Non Churner' AND fmc_s_fla_ChurnType = 'Involuntary Churner') THEN 'Involuntary Churner'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 0) AND (fmc_s_fla_ChurnFlag <> 'Non Churner' AND fmc_s_fla_ChurnType = 'Mixed Churner') THEN 'Mixed Churner'
                WHEN (fmc_b_att_active = 1 and fmc_e_att_active = 0) AND fix_s_fla_MainMovement = '6.Null last day' AND fmc_s_fla_ChurnFlag = 'Non Churner' THEN 'Loss-Fixed Customer Gap'
                    ELSE NULL END AS fmc_s_fla_waterfall
FROM FMC_base_adj
)

SELECT  *
FROM final_flags
WHERE fmc_b_att_active + fmc_e_att_active >= 1
    and mob_s_att_duplicates = 1
