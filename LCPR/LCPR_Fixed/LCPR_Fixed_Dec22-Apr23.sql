-----------------------------------------------------------------------------------------
---------------------------- LCPR FIXED TABLE - V1 --------------------------------------
-----------------------------------------------------------------------------------------
--- Comment (2023-07-20): Use code for obtaining fixed results for April 2023 and before ---


--CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_fixed_table_jan_feb28" AS

WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-04-01')) AS input_month
        ,85 as overdue_days
)

,useful_fields AS (
SELECT  date(dt) AS dt
        ,DATE(as_of) AS as_of
        ,sub_acct_no_sbb AS fix_s_att_account
        ,home_phone_sbb AS phone1
        ,bus_phone_sbb AS phone2
        ,delinquency_days AS overdue
        ,(CAST(CAST(first_value(connect_dte_sbb) over (PARTITION BY sub_acct_no_sbb order by DATE(dt) DESC) AS TIMESTAMP) AS DATE)) AS max_start
        ,(video_chrg + hsd_chrg + voice_chrg) AS TOTAL_MRC
        ,IF(TRIM(drop_type) = 'FIBER','FTTH',IF(TRIM(drop_type) = 'FIBCO','FIBCO',IF(TRIM(drop_type) = 'COAX','HFC',null))) AS tech_flag
        ,hsd AS numBB
        ,video AS numTV
        ,voice AS numVO
        ,null AS oldest_unpaid_bill_dt
        ,first_value(delinquency_days) over(PARTITION BY sub_acct_no_sbb,date_trunc('month',date(dt)) ORDER BY date(dt) DESC) AS last_overdue
        ,joint_customer
        ,welcome_offer
        ,bill_code
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE play_type <> '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) BETWEEN ((SELECT input_month FROM parameters) + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  ((SELECT input_month FROM parameters) + interval '1' MONTH) 
)

,BOM_subsidized_customers AS (
SELECT  sub_acct_no_sbb
FROM "lcpr.stage.dev"."lcpr_fix_subsidized_customers"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' MONTH
)

,EOM_subsidized_customers AS (
SELECT  sub_acct_no_sbb
FROM "lcpr.stage.dev"."lcpr_fix_subsidized_customers"
WHERE date(dt) = (SELECT input_month FROM parameters)
)

,BOM_active_base AS (
SELECT  DATE_TRUNC('MONTH',dt) AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_b_dim_date
        ,phone1
        ,phone2
        ,tech_flag AS fix_b_fla_tech
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_b_fla_MixCodeAdj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_b_fla_MixNameAdj
        ,0 AS fix_b_att_bbCode
        ,0 AS fix_b_att_tvCode
        ,0 AS fix_b_att_voCode
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_b_fla_BB
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_b_fla_TV
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_b_fla_VO
        ,(numBB + numTV + numVO) AS fix_b_mes_numRGUS
        ,total_MRC AS fix_b_mes_mrc
        ,overdue AS fix_b_mes_overdue
        ,max_start AS fix_b_att_MaxStart
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_b_fla_tenure
        ,IF(welcome_offer = 'X','1.Real FMC',IF(joint_customer = 'X','2.Near FMC','3.Fixed Only')) AS fix_b_fla_fmc
        ,IF(fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM BOM_subsidized_customers),1,0) AS fix_b_fla_subsidized
        ,bill_code AS fix_b_att_BillCode
FROM useful_fields
WHERE dt = date_trunc('MONTH',dt)
    AND (CAST(overdue AS INTEGER) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL)
)

,EOM_active_base AS (
SELECT  DATE_TRUNC('MONTH',(dt - interval '1' day)) AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_e_dim_date
        ,phone1
        ,phone2
        ,tech_flag AS fix_e_fla_tech
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_e_fla_MixCodeAdj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_e_fla_MixNameAdj
        ,0 AS fix_e_att_bbCode
        ,0 AS fix_e_att_tvCode
        ,0 AS fix_e_att_voCode
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_e_fla_BB
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_e_fla_TV
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_e_fla_VO
        ,(numBB + numTV + numVO) AS fix_e_mes_numRGUS
        ,total_MRC AS fix_e_mes_mrc
        ,overdue AS fix_e_mes_overdue
        ,max_start AS fix_e_att_MaxStart
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_e_fla_tenure
        ,IF(welcome_offer = 'X','1.Real FMC',IF(joint_customer = 'X','2.Near FMC','3.Fixed Only')) AS fix_e_fla_fmc
        ,IF(fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM EOM_subsidized_customers),1,0) AS fix_e_fla_subsidized
        ,bill_code AS fix_e_att_BillCode
FROM useful_fields
WHERE dt = date_trunc('MONTH',dt)
    AND (CAST(overdue AS INTEGER) < (SELECT overdue_days FROM parameters) OR overdue IS NULL)
)

,customer_status AS (
SELECT  CASE    WHEN (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NOT NULL) OR (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NULL) THEN A.fix_s_dim_month
                WHEN (A.fix_s_att_account IS NULL AND B.fix_s_att_account IS NOT NULL) THEN B.fix_s_dim_month
                    END AS fix_s_dim_month
        ,CASE WHEN (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NOT NULL) OR (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NULL) THEN A.fix_s_att_account
        WHEN (A.fix_s_att_account IS NULL AND B.fix_s_att_account IS NOT NULL) THEN B.fix_s_att_account
                    END AS fix_s_att_account
        ,IF(A.fix_s_att_account IS NOT NULL,1,0) AS fix_b_att_active
        ,IF(B.fix_s_att_account IS NOT NULL,1,0) AS fix_e_att_active
        ,IF(A.phone1 IS NOT NULL AND A.phone1 <> 0,A.phone1,IF(B.phone1 IS NOT NULL AND B.phone1 <> 0,B.phone1,NULL)) AS fix_s_att_phone1
        ,IF(A.phone2 IS NOT NULL AND A.phone2 <> 0,A.phone2,IF(B.phone2 IS NOT NULL AND B.phone2 <> 0,B.phone2,NULL)) AS fix_s_att_phone2
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
FROM BOM_active_base A FULL OUTER JOIN EOM_active_base b 
    ON A.fix_s_att_account = B.fix_s_att_account AND A.fix_s_dim_month = B.fix_s_dim_month
)

,main_movement_flag AS (
SELECT  A.*
        ,CASE   WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) = 0 THEN '1.SameRGUs'
                WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) > 0 THEN '2.Upsell'
                WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) < 0 THEN '3.Downsell'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) =  fix_s_dim_month) THEN '4.New Customer'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) <> fix_s_dim_month) THEN '5.Come Back to Life'
                WHEN (fix_b_mes_numRGUS > 0 AND fix_e_mes_numRGUS IS NULL) THEN '6.Null last day'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS IS NULL) THEN '7.Always null'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) is null) THEN '8.Rejoiner-GrossAdd Gap'
                    END AS fix_s_fla_MainMovement
FROM customer_status A
)

,spin_movement_flag AS (
SELECT  A.*
        ,CASE   WHEN fix_s_fla_MainMovement = '1.SameRGUs' AND (fix_e_mes_mrc - fix_b_mes_mrc) > 0 THEN '1. Up-spin'
                WHEN fix_s_fla_MainMovement = '1.SameRGUs' AND (fix_e_mes_mrc - fix_b_mes_mrc) < 0 THEN '2. Down-spin'
                ELSE '3. No Spin' 
                    END AS fix_s_fla_SpinMovement
FROM main_movement_flag A
)

--------------------------------- VOLUNTARY CHURN ---------------------------------------
,service_orders_flag AS (
SELECT  DATE_TRUNC('MONTH',DATE(completed_date)) AS month
        ,DATE(completed_date) AS end_date
        ,DATE(order_start_date) AS start_date
        ,cease_reason_code
        ,cease_reason_desc
        ,order_type
        ,CASE   WHEN cease_reason_desc = 'MIG COAX TO FIB' THEN 'Migracion'
                WHEN cease_reason_desc = 'NON-PAY' THEN 'Involuntario'
                ELSE 'Voluntario' END AS dx_type
        ,account_id
        ,lob_vo_count
        ,lob_bb_count
        ,lob_tv_count
        ,IF(lob_vo_count > 0,1,0) AS VO_Churn
        ,IF(lob_bb_count > 0,1,0) AS BB_Churn
        ,IF(lob_tv_count > 0,1,0) AS TV_Churn
        ,(IF(lob_vo_count > 0,1,0)+IF(lob_bb_count > 0,1,0)+IF(lob_tv_count > 0,1,0)) AS RGUs_Prel
FROM "lcpr.stage.prod"."so_hdr_lcpr"
WHERE order_type = 'V_DISCO'
    AND account_type = 'RES'
    AND order_status = 'COMPLETE'
)

,churned_RGUs_SO AS (
SELECT  month
        ,account_id
        ,dx_type
        ,SUM(RGUs_Prel) AS churned_RGUs
FROM service_orders_flag
GROUP BY 1,2,3
)

,RGUS_MixLastDay AS (
SELECT  DATE_TRUNC('MONTH',dt) AS month
        ,dt
        ,fix_s_att_account
        ,overdue
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS MixName_Adj
FROM useful_fields
)

,RGUs_LastRecord_DNA AS (
SELECT  DISTINCT month
        ,fix_s_att_account
        ,first_value(MixName_Adj) OVER (PARTITION BY fix_s_att_account,month ORDER BY dt DESC) AS last_RGU
FROM RGUS_MixLastDay
WHERE (cast(overdue as double) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL)
)

,RGUS_LastRecord_DNA_Adj AS (
SELECT  month
        ,fix_s_att_account
        ,last_RGU
        ,CASE   WHEN last_RGU IN ('VO','BB','TV') THEN 1
                WHEN last_RGU IN ('BB+VO', 'BB+TV', 'VO+TV') THEN 2
                WHEN last_RGU IN ('BB+VO+TV') THEN 3
                WHEN last_RGU IN ('0P') THEN -1
                    ELSE 0 END AS NumRGUs_LastRecord
FROM RGUs_LastRecord_DNA
)

,date_LastRecord_DNA AS (
SELECT  DATE_TRUNC('MONTH',dt) AS month
        ,fix_s_att_account
        ,MAX(dt) AS last_date
FROM useful_fields
WHERE (cast(overdue as double) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL)
GROUP BY 1,2
)

,overdue_LastRecord_DNA AS (
SELECT  DATE_TRUNC('MONTH',A.dt) AS month
        ,A.fix_s_att_account
        ,A.overdue AS LastOverdueRecord
        ,(DATE_DIFF('DAY',DATE(A.max_start),A.dt)) AS ChurnTenureDays
FROM useful_fields A INNER JOIN date_LastRecord_DNA B ON A.fix_s_att_account = B.fix_s_att_account AND A.dt = B.last_date
)

,voluntary_flag AS(
SELECT  B.month
        ,B.fix_s_att_account
        ,A.dx_type
        ,B.Last_RGU
        ,B.NumRGUs_LastRecord
        ,A.churned_RGUs
        ,IF(A.churned_RGUs >= B.NumRGUs_LastRecord,1,0) AS vol_flag
FROM churned_RGUs_SO A
INNER JOIN RGUs_LastRecord_DNA_Adj B    ON A.account_id = B.fix_s_att_account AND A.month = B.month
INNER JOIN date_LastRecord_DNA C        ON B.fix_s_att_account = C.fix_s_att_account AND B.month = date_trunc('month',C.last_date)
INNER JOIN overdue_LastRecord_DNA D     ON B.fix_s_att_account = D.fix_s_att_account AND B.month = D.month
)

,voluntary_churners AS (
SELECT  DISTINCT A.fix_s_dim_month
        ,A.fix_s_att_account
        ,A.fix_b_mes_numRGUS
        ,A.fix_e_att_active
        ,B.last_RGU
        ,B.churned_RGUs
        ,B.NumRGUs_LastRecord
        ,IF(B.fix_s_att_account IS NOT NULL AND B.vol_flag = 1,'Voluntario',null) AS ChurnType
FROM spin_movement_flag A LEFT JOIN voluntary_flag B ON A.fix_s_att_account = B.fix_s_att_account AND A.fix_s_dim_month = B.month
)

,voluntary_churners_adj AS (
SELECT  DISTINCT fix_s_dim_month AS month
        ,fix_s_att_account AS churn_account
        ,ChurnType
        ,fix_e_att_active
        ,IF(ChurnType IS NOT NULL AND fix_e_att_active = 1 AND fix_b_mes_numRGUS > NumRGUs_LastRecord,1,0) AS partial_churn
FROM voluntary_churners
)

,final_voluntary_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,IF(churn_account IS NOT NULL AND (fix_e_att_active = 0 OR fix_e_att_active IS NULL),'1. Fixed Voluntary Churner',NULL) AS fix_s_fla_ChurnType
FROM voluntary_churners_adj
WHERE ChurnType IS NOT NULL AND partial_churn = 0 
)

-------------------------------- INVOLUNTARY CHURN --------------------------------------
,first_cust_record AS (
SELECT  DATE_TRUNC('MONTH',DATE_ADD('MONTH',1,DATE(dt))) AS mes
        ,fix_s_att_account
        ,MIN(date(dt)) AS first_cust_record
        ,DATE_ADD('day',-1,MIN(date(dt))) AS prev_first_cust_record
FROM useful_fields
WHERE date(dt) = date_trunc('MONTH', DATE(dt)) + interval '1' MONTH - interval '1' day
GROUP BY 1,2
)

,last_cust_record AS (
SELECT  DATE_TRUNC('MONTH',DATE(dt)) AS mes
        ,fix_s_att_account
        ,MAX(date(dt)) AS last_cust_record
        ,DATE_ADD('day',-1,MAX(date(dt))) AS prev_last_cust_record
        ,DATE_ADD('day',-2,MAX(date(dt))) AS prev_last_cust_record2
FROM useful_fields
GROUP BY 1,2
ORDER BY 1,2
)

,no_overdue AS (
SELECT  DATE_TRUNC('MONTH', DATE_ADD('MONTH',1, DATE(A.dt))) AS MES
        ,A.fix_s_att_account
        ,A.overdue
FROM useful_fields A INNER JOIN first_cust_record B ON A.fix_s_att_account = B.fix_s_att_account
WHERE (CAST(A.overdue as INT) < (SELECT overdue_days FROM parameters))
    AND (date(A.dt) = B.first_cust_record or date(A.dt) = B.prev_first_cust_record)
GROUP BY 1,2,3
)

,overdue_last_day AS (
SELECT  DATE_TRUNC('MONTH',DATE(dt)) AS MES
        ,A.fix_s_att_account
        ,A.overdue
        ,(DATE_DIFF('DAY',DATE(dt),max_start)) AS churn_tenure_days
FROM useful_fields A INNER JOIN last_cust_record B ON A.fix_s_att_account = B.fix_s_att_account
WHERE date(A.dt) IN (B.last_cust_record,B.prev_last_cust_record,B.prev_last_cust_record2)
    AND (CAST(A.overdue AS INTEGER) >= (SELECT overdue_days FROM parameters))
GROUP BY 1,2,3,4
)

,involuntary_net_churners AS(
SELECT  DISTINCT A.mes AS month
        ,A.fix_s_att_account
        ,B.churn_tenure_days
FROM no_overdue A INNER JOIN overdue_last_day B ON A.fix_s_att_account = B.fix_s_att_account and A.mes = B.mes
)

,involuntary_churners AS (
SELECT  DISTINCT A.month
        ,A.fix_s_att_account AS churn_account
        ,A.churn_tenure_days
        ,CASE WHEN A.fix_s_att_account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS fix_s_fla_ChurnType
FROM involuntary_net_churners A LEFT JOIN useful_fields B on A.fix_s_att_account = B.fix_s_att_account AND A.month = DATE_TRUNC('month',date(B.dt))
where last_overdue >= (SELECT overdue_days FROM parameters)
GROUP BY 1,2,4,3
)

,final_involuntary_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,fix_s_fla_ChurnType
FROM involuntary_churners
WHERE fix_s_fla_ChurnType = '2. Fixed Involuntary Churner'
)

,all_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,fix_s_fla_ChurnType
FROM    (SELECT month,churn_account,fix_s_fla_ChurnType FROM final_voluntary_churners A 
         UNION ALL
         SELECT month,churn_account,fix_s_fla_ChurnType FROM final_involuntary_churners B
        )
)

,fixed_table_churn_flag AS(
SELECT  A.*
        ,CASE   WHEN B.churn_account IS NOT NULL AND fix_s_fla_MainMovement = '6.Null last day' THEN '1. Fixed Churner'
                --WHEN B.churn_account IS NULL AND fix_s_fla_MainMovement <> '6.Null last day' THEN '2. Fixed NonChurner'
                ELSE '2. Fixed NonChurner'
                    END AS fix_s_fla_ChurnFlag
        ,CASE   WHEN B.churn_account IS NOT NULL THEN fix_s_fla_ChurnType
                    ELSE NULL END AS fix_s_fla_ChurnType
FROM spin_movement_flag A LEFT JOIN all_churners B ON A.fix_s_att_account = B.churn_account AND A.fix_s_dim_month = B.month
)

------------------------------------ REJOINERS ------------------------------------------

,inactive_users AS (
SELECT  DISTINCT fix_s_dim_month AS exit_month
        ,fix_s_att_account AS account
        ,DATE_ADD('MONTH',1,date(fix_s_dim_month)) AS rejoiner_month
FROM customer_status
WHERE fix_b_att_active = 1 AND fix_e_att_active = 0
)

,rejoiner_population AS (
SELECT  A.fix_s_dim_month
        ,A.fix_s_att_account
        ,B.rejoiner_month
        ,IF(B.account IS NOT NULL,1,0) AS rejoiner_pop_flag
        ,IF((B.rejoiner_month >= (SELECT input_month FROM parameters) AND B.rejoiner_month <= DATE_ADD('MONTH',1,(SELECT input_month FROM parameters))),1,0) AS fix_s_fla_PRMonth
FROM fixed_table_churn_flag A LEFT JOIN inactive_users B
    ON A.fix_s_att_account = B.account AND A.fix_s_dim_month = B.exit_month
)

,fixed_rejoiner_month_population AS (
SELECT  DISTINCT fix_s_dim_month
        ,rejoiner_pop_flag
        ,fix_s_fla_PRMonth
        ,fix_s_att_account
        ,(SELECT input_month FROM parameters) AS month
FROM rejoiner_population
WHERE rejoiner_pop_flag = 1
        AND fix_s_fla_PRMonth = 1
        AND fix_s_dim_month <> (SELECT input_month FROM parameters)
GROUP BY 1,2,3,4
)

,month_fixed_rejoiners AS (
SELECT  A.*
        ,IF(fix_s_fla_PRMonth = 1 AND fix_s_fla_MainMovement = '5.Come Back to Life',1,0) AS fix_s_fla_Rejoiner
FROM fixed_table_churn_flag A LEFT JOIN fixed_rejoiner_month_population B
    ON A.fix_s_att_account = B.fix_s_att_account AND A.fix_s_dim_month = CAST(B.month AS DATE)
)

,invol_flag_SO AS (
SELECT  DISTINCT A.*
        ,IF(fix_s_fla_ChurnFlag = '2. Fixed NonChurner' AND fix_e_att_active = 0,'Churner Gap',NULL) AS Gap
        ,IF(dx_type = 'Involuntario' AND fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_e_att_active = 0,1,0) AS early_dx_flag
        ,IF(dx_type = 'Migracion' AND fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_e_att_active = 0,1,0) AS migrt_flag
FROM month_fixed_rejoiners A LEFT JOIN service_orders_flag B 
    ON A.fix_s_att_account = B.account_id  AND A.fix_s_dim_month = DATE_TRUNC('MONTH',B.end_date) 
)

,prepaid_churners AS (
SELECT  DISTINCT DATE(date_trunc('MONTH',DATE(dt))) AS month
        ,date(dt) AS dt
        ,sub_acct_no_sbb
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE play_type = '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) BETWEEN ((SELECT input_month FROM parameters) + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  ((SELECT input_month FROM parameters) + interval '1' MONTH) 
)

,prepaid_churner_flag AS(
SELECT  DISTINCT A.*
        ,IF(A.fix_b_fla_MixCodeAdj IS NOT NULL AND B.sub_acct_no_sbb IS NOT NULL,'Churner0P',NULL) AS churn0p
FROM invol_flag_SO A LEFT JOIN prepaid_churners B 
    ON A.fix_s_dim_month = B.month AND A.fix_s_att_account = B.sub_acct_no_sbb
)

,transfers AS (
SELECT account_id
FROM "lcpr.stage.prod"."so_hdr_lcpr"
WHERE TRIM(order_type) = 'RELOCATION/TRAN'
    AND account_type = 'RES'
    AND order_status = 'COMPLETE'
    AND DATE_TRUNC('MONTH',DATE(dt)) = (SELECT input_month FROM parameters) 
)

,final_fixed_flags AS (
SELECT  fix_s_dim_month
        ,fix_s_att_account
        ,fix_b_att_active
        ,IF(fix_s_fla_ChurnType = '2. Fixed Involuntary Churner',0,fix_e_att_active) AS fix_e_att_active
        ,fix_s_att_phone1
        ,if(fix_s_att_phone2 > 100,fix_s_att_phone2,NULL) AS fix_s_att_phone2
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
        ,IF(fix_s_fla_ChurnType = '2. Fixed Involuntary Churner',0,fix_e_mes_numRGUS) AS fix_e_mes_numRGUS
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
        ,IF(fix_s_fla_ChurnType = '2. Fixed Involuntary Churner','6.Null last day',IF(fix_s_att_account IN (SELECT distinct account_id FROM transfers) AND fix_s_fla_MainMovement IN('4.New Customer','5.Come Back to Life'),'7.Transfer Adds',fix_s_fla_MainMovement)) AS fix_s_fla_MainMovement
        ,IF(fix_s_fla_ChurnType IS NOT NULL,'3. No Spin',fix_s_fla_SpinMovement) AS fix_s_fla_SpinMovement
        ,CASE   WHEN (early_dx_flag + migrt_flag) >= 1 THEN '1. Fixed Churner'
                WHEN fix_s_fla_ChurnFlag = '2. Fixed NonChurner' AND fix_b_att_active = 1 AND fix_e_att_active = 0 /*AND churn0p = 'Churner0P'*/ THEN '1. Fixed Churner'
                WHEN fix_s_fla_ChurnType IS NOT NULL THEN '1. Fixed Churner'
                    ELSE fix_s_fla_ChurnFlag END AS fix_s_fla_ChurnFlag
        ,CASE   WHEN fix_s_fla_MainMovement = '6.Null last day' AND fix_s_fla_ChurnType IS NULL THEN '3. Fixed Transfer'
                WHEN early_dx_flag = 1 THEN '2. Fixed Involuntary Churner'
                WHEN migrt_flag = 1 THEN '1. Fixed Voluntary Churner'
                WHEN fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_b_att_active = 1 AND fix_e_att_active = 0 AND churn0p = 'Churner0P' THEN '4. Fixed 0P Churner'
                    ELSE fix_s_fla_ChurnType END AS fix_s_fla_ChurnType
        ,CASE   WHEN fix_s_fla_MainMovement = '6.Null last day' AND fix_s_fla_ChurnType IS NULL THEN 'Transfer'
                WHEN early_dx_flag = 1 THEN 'Early Dx'
                WHEN migrt_flag = 1 THEN 'Incomplete CST'
                WHEN fix_s_fla_ChurnType = '1. Fixed Voluntary Churner' THEN 'Voluntary'
                WHEN fix_s_fla_ChurnType = '2. Fixed Involuntary Churner' THEN 'Involuntary'
                WHEN fix_s_fla_ChurnFlag = '2. Fixed NonChurner' AND fix_b_att_active = 1 AND fix_e_att_active = 0 AND churn0p = 'Churner0P' THEN '0P Churner'
                    END AS fix_s_fla_ChurnSubType
        ,fix_s_fla_Rejoiner
FROM prepaid_churner_flag
)

SELECT *
FROM final_fixed_flags
WHERE fix_s_dim_month = (SELECT input_month FROM parameters)
    AND fix_b_att_active + fix_e_att_active >= 1

--- --- --- ### ### Quick check

-- select  fix_s_dim_month
--         ,fix_b_att_active
--         ,fix_e_att_active
--         ,fix_s_fla_MainMovement
--         ,fix_s_fla_SpinMovement
--         ,fix_s_fla_ChurnFlag
--         ,fix_s_fla_ChurnType
--         ,count(distinct fix_s_att_account) as accounts
--         ,sum(fix_b_mes_numRGUS) as BOM_RGUS
--         ,sum(fix_e_mes_numRGUS) as EOM_RGUS
-- FROM final_fixed_flags
-- WHERE fix_s_dim_month = (SELECT input_month FROM parameters)
-- GROUP BY 1,2,3,4,5,6,7
-- ORDER BY 1,4,5,6,7
