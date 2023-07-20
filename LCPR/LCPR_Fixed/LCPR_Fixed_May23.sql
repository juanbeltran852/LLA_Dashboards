-----------------------------------------------------------------------------------------
---------------------------- LCPR FIXED TABLE - V2 --------------------------------------
-----------------------------------------------------------------------------------------
--- Comment (2023-07-20): Use code for obtaining fixed results for May 2023 ---

--- --- Adjustments (18/06/2023):
--- 1. Order Activity logics are used to complement some of the logics already used (mainly for Vol Churn and Transfers).
--- 2. Involuntary churns cannot be subsidized customers.
--- 3. Subsidized customers in overdue are considered as part of the opening base and the closing base.

--CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_fixed_table_jan_feb28" AS

WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-05-01')) AS input_month
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
        ,acp
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE play_type <> '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) BETWEEN ((SELECT input_month FROM parameters) + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  ((SELECT input_month FROM parameters) + interval '1' MONTH)
)

,BOM_subsidized_customers AS (
SELECT  sub_acct_no_sbb
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' MONTH
AND UPPER(acp) = 'X'
)

,EOM_subsidized_customers AS (
SELECT  sub_acct_no_sbb
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE date(dt) = (SELECT input_month FROM parameters)
AND UPPER(acp) = 'X'
)

, bom_subsidized_in_overdue as (
SELECT
    sub_acct_no_sbb as account_id
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    play_type <> '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) = (SELECT input_month FROM parameters) -- - interval '1' day
    AND delinquency_days > 85
    AND acp is not null
)

, eom_subsidized_in_overdue as (
SELECT
    distinct sub_acct_no_sbb as account_id
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE 
    play_type <> '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) = (SELECT input_month FROM parameters) + interval '1' month 
    AND delinquency_days > 85
    AND acp is not null
)

,BOM_active_base AS (
SELECT  DATE_TRUNC('MONTH',dt) AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_b_dim_date
        ,phone1
        ,phone2
        ,tech_flag AS fix_b_fla_tech_type
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_b_dim_mix_code_adj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_b_att_mix_name_adj
        ,0 AS fix_b_dim_bb_code
        ,0 AS fix_b_dim_tv_code
        ,0 AS fix_b_dim_vo_code
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_b_fla_bb_rgu
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_b_fla_tv_rgu
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_b_fla_vo_rgu
        ,(numBB + numTV + numVO) AS fix_b_mes_num_rgus
        ,total_MRC AS fix_b_mes_mrc
        ,overdue AS fix_b_mes_outstage
        ,max_start AS fix_b_dim_max_start
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_b_fla_tenure
        ,IF(welcome_offer = 'X','1.Real FMC',IF(joint_customer = 'X','2.Near FMC','3.Fixed Only')) AS fix_b_fla_fmc
        ,IF(/*fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM BOM_subsidized_customers)*/ acp is not null,1,0) AS fix_b_fla_subsidized
        ,bill_code AS fix_b_att_BillCode
FROM useful_fields
WHERE 
    dt = date_trunc('MONTH',dt)
    AND ((CAST(overdue AS INTEGER) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL) /*OR fix_s_att_account in (SELECT account_id FROM bom_subsidized_in_overdue)*/ )
    
UNION ALL --- Adding users that started the month in overdue but are subsidized, so they are part of the opening base

SELECT  DATE_TRUNC('MONTH',dt) AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_b_dim_date
        ,phone1
        ,phone2
        ,tech_flag AS fix_b_fla_tech_type
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_b_dim_mix_code_adj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_b_att_mix_name_adj
        ,0 AS fix_b_dim_bb_code
        ,0 AS fix_b_dim_tv_code
        ,0 AS fix_b_dim_vo_code
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_b_fla_bb_rgu
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_b_fla_tv_rgu
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_b_fla_vo_rgu
        ,(numBB + numTV + numVO) AS fix_b_mes_num_rgus
        ,total_MRC AS fix_b_mes_mrc
        ,overdue AS fix_b_mes_outstage
        ,max_start AS fix_b_dim_max_start
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_b_fla_tenure
        ,IF(welcome_offer = 'X','1.Real FMC',IF(joint_customer = 'X','2.Near FMC','3.Fixed Only')) AS fix_b_fla_fmc
        ,IF(/*fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM BOM_subsidized_customers)*/ acp is not null,1,0) AS fix_b_fla_subsidized
        ,bill_code AS fix_b_att_BillCode
FROM useful_fields
WHERE
    date(dt) = (SELECT input_month FROM parameters)
    and fix_s_att_account in (SELECT account_id FROM bom_subsidized_in_overdue)
)

,EOM_active_base AS (

SELECT  DATE_TRUNC('MONTH',(dt - interval '1' day)) AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_e_dim_date
        ,phone1
        ,phone2
        ,tech_flag AS fix_e_fla_tech_type
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_e_fla_MixCodeAdj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_e_att_mix_name_adj
        ,0 AS fix_e_dim_bb_code
        ,0 AS fix_e_dim_tv_code
        ,0 AS fix_e_dim_vo_code
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_e_dim_bb_rgu
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_e_dim_tv_rgu
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_e_dim_vo_rgu
        ,(numBB + numTV + numVO) AS fix_e_mes_num_rgus
        ,total_MRC AS fix_e_mes_mrc
        ,overdue AS fix_e_mes_outstage
        ,max_start AS fix_e_dim_max_start
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_e_fla_tenure
        ,IF(welcome_offer = 'X','1.Real FMC',IF(joint_customer = 'X','2.Near FMC','3.Fixed Only')) AS fix_e_fla_fmc
        ,IF(/*fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM EOM_subsidized_customers)*/ acp is not null,1,0) AS fix_e_fla_subsidized
        ,bill_code AS fix_e_att_BillCode
FROM useful_fields
WHERE 
    dt = date_trunc('MONTH',dt)
    AND (CAST(overdue AS INTEGER) < (SELECT overdue_days FROM parameters) OR overdue IS NULL) /*OR fix_s_att_account in (SELECT account_id FROM eom_subsidized_in_overdue)*/

UNION ALL --- Adding subsidized customers in overdue at the end of the month as active base EOM.

SELECT  DATE_TRUNC('MONTH',(dt - interval '1' day)) AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_e_dim_date
        ,phone1
        ,phone2
        ,tech_flag AS fix_e_fla_tech_type
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_e_fla_MixCodeAdj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_e_att_mix_name_adj
        ,0 AS fix_e_dim_bb_code
        ,0 AS fix_e_dim_tv_code
        ,0 AS fix_e_dim_vo_code
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_e_dim_bb_rgu
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_e_dim_tv_rgu
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_e_dim_vo_rgu
        ,(numBB + numTV + numVO) AS fix_e_mes_num_rgus
        ,total_MRC AS fix_e_mes_mrc
        ,overdue AS fix_e_mes_outstage
        ,max_start AS fix_e_dim_max_start
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_e_fla_tenure
        ,IF(welcome_offer = 'X','1.Real FMC',IF(joint_customer = 'X','2.Near FMC','3.Fixed Only')) AS fix_e_fla_fmc
        ,IF(/*fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM EOM_subsidized_customers)*/ acp is not null,1,0) AS fix_e_fla_subsidized
        ,bill_code AS fix_e_att_BillCode
FROM useful_fields
WHERE
    date(dt) = (SELECT input_month FROM parameters) + interval '1' month
    and fix_s_att_account in (SELECT account_id FROM eom_subsidized_in_overdue)

)

,customer_status AS (
SELECT  CASE    WHEN (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NOT NULL) OR (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NULL) THEN A.fix_s_dim_month
                WHEN (A.fix_s_att_account IS NULL AND B.fix_s_att_account IS NOT NULL) THEN B.fix_s_dim_month
                    END AS fix_s_dim_month
        ,CASE WHEN (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NOT NULL) OR (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NULL) THEN A.fix_s_att_account
        WHEN (A.fix_s_att_account IS NULL AND B.fix_s_att_account IS NOT NULL) THEN B.fix_s_att_account
                    END AS fix_s_att_account
        ,IF(A.fix_s_att_account IS NOT NULL,1,0) AS fix_b_fla_active
        ,IF(B.fix_s_att_account IS NOT NULL,1,0) AS fix_e_fla_active
        ,IF(A.phone1 IS NOT NULL AND A.phone1 <> 0,A.phone1,IF(B.phone1 IS NOT NULL AND B.phone1 <> 0,B.phone1,NULL)) AS fix_s_att_contact_phone1
        
        ,IF(A.phone2 IS NOT NULL AND A.phone2 <> 0,A.phone2,IF(B.phone2 IS NOT NULL AND B.phone2 <> 0,B.phone2,NULL)) AS fix_s_att_contact_phone2
        ,fix_b_dim_date
        ,fix_b_mes_outstage
        ,fix_b_dim_max_start
        ,fix_b_fla_tenure
        ,fix_b_mes_mrc
        ,fix_b_fla_tech_type
        ,fix_b_fla_fmc
        ,fix_b_mes_num_rgus
        ,fix_b_att_mix_name_adj
        ,fix_b_dim_mix_code_adj
        ,fix_b_fla_bb_rgu
        ,fix_b_fla_tv_rgu
        ,fix_b_fla_vo_rgu
        ,fix_b_dim_bb_code
        ,fix_b_dim_tv_code
        ,fix_b_dim_vo_code
        ,fix_b_fla_subsidized
        ,fix_b_att_BillCode
        ,fix_e_dim_date
        ,fix_e_mes_outstage
        ,fix_e_dim_max_start
        ,fix_e_fla_tenure
        ,fix_e_mes_mrc
        ,fix_e_fla_tech_type
        ,fix_e_fla_fmc
        ,fix_e_mes_num_rgus
        ,fix_e_att_mix_name_adj
        ,fix_e_fla_MixCodeAdj
        ,fix_e_dim_bb_rgu
        ,fix_e_dim_tv_rgu
        ,fix_e_dim_vo_rgu
        ,fix_e_dim_bb_code
        ,fix_e_dim_tv_code
        ,fix_e_dim_vo_code
        ,fix_e_fla_subsidized
        ,fix_e_att_BillCode
FROM BOM_active_base A FULL OUTER JOIN EOM_active_base b 
    ON A.fix_s_att_account = B.fix_s_att_account AND A.fix_s_dim_month = B.fix_s_dim_month
)

,main_movement_flag AS (
SELECT  A.*
        ,CASE   WHEN (fix_e_mes_num_rgus - fix_b_mes_num_rgus) = 0 THEN '1.SameRGUs'
                WHEN (fix_e_mes_num_rgus - fix_b_mes_num_rgus) > 0 THEN '2.Upsell'
                WHEN (fix_e_mes_num_rgus - fix_b_mes_num_rgus) < 0 THEN '3.Downsell'
                WHEN (fix_b_mes_num_rgus IS NULL AND fix_e_mes_num_rgus > 0 AND DATE_TRUNC ('MONTH', fix_e_dim_max_start) =  fix_s_dim_month) THEN '4.New Customer'
                WHEN (fix_b_mes_num_rgus IS NULL AND fix_e_mes_num_rgus > 0 AND DATE_TRUNC ('MONTH', fix_e_dim_max_start) <> fix_s_dim_month) THEN '5.Come Back to Life'
                WHEN (fix_b_mes_num_rgus > 0 AND fix_e_mes_num_rgus IS NULL) THEN '6.Null last day'
                WHEN (fix_b_mes_num_rgus IS NULL AND fix_e_mes_num_rgus IS NULL) THEN '7.Always null'
                WHEN (fix_b_mes_num_rgus IS NULL AND fix_e_mes_num_rgus > 0 AND DATE_TRUNC ('MONTH', fix_e_dim_max_start) is null) THEN '8.Rejoiner-GrossAdd Gap'
                    END AS fix_s_fla_main_movement
FROM customer_status A
)

,spin_movement_flag AS (
SELECT  A.*
        ,CASE   WHEN fix_s_fla_main_movement = '1.SameRGUs' AND (fix_e_mes_mrc - fix_b_mes_mrc) > 0 THEN '1. Up-spin'
                WHEN fix_s_fla_main_movement = '1.SameRGUs' AND (fix_e_mes_mrc - fix_b_mes_mrc) < 0 THEN '2. Down-spin'
                ELSE '3. No Spin' 
                    END AS fix_s_fla_spin_movement
FROM main_movement_flag A
)

--------------------------------- RELEVANT GROUPS FROM ORDER ACTIVITY -------------------

, intramonth_churn as ( --- Customers that did churn but were reconnected in the same month.
SELECT
    sub_acct_no_ooi as account_id
FROM "db-stage-dev"."order_activity_lcpr"
WHERE 
    date_trunc('month', date(SUBSTRING(ls_chg_dte_ocr, LENGTH(ls_chg_dte_ocr) - 10 + 1))) = (SELECT input_month FROM parameters) 
    and acct_type = 'R' 
    and ord_typ in ('V_DISCO', 'NON PAY')
    and sub_acct_no_ooi in (SELECT sub_acct_no_ooi FROM "db-stage-dev"."order_activity_lcpr" WHERE date_trunc('month', date(SUBSTRING(ls_chg_dte_ocr, LENGTH(ls_chg_dte_ocr) - 10 + 1))) = (SELECT input_month FROM parameters) 
        and acct_type = 'R' and ord_typ = 'RESTART')
)

, oa_vol_churn as (
SELECT
    sub_acct_no_ooi as account_id
FROM "db-stage-dev"."order_activity_lcpr"
WHERE 
    date_trunc('month', date(SUBSTRING(ls_chg_dte_ocr, LENGTH(ls_chg_dte_ocr) - 10 + 1))) = (SELECT input_month FROM parameters) 
    and acct_type = 'R' 
    and ord_typ = 'V_DISCO' and disco_rsn_sbb != 'VL' --- Excluding transfers
    and sub_acct_no_ooi not in (SELECT account_id FROM intramonth_churn) --- Excluding recovered churns
)

, oa_transfers as (
SELECT
    sub_acct_no_ooi as account_id
FROM "db-stage-dev"."order_activity_lcpr"
WHERE 
    date_trunc('month', date(SUBSTRING(ls_chg_dte_ocr, LENGTH(ls_chg_dte_ocr) - 10 + 1))) = (SELECT input_month FROM parameters) 
    and acct_type = 'R' 
    and ord_typ = 'V_DISCO' and order_rsn = 'VL'
)

, oa_transfer_adds as (
SELECT
    sub_acct_no_ooi as account_id
FROM "db-stage-dev"."order_activity_lcpr"
WHERE 
    date_trunc('month', date(SUBSTRING(ls_chg_dte_ocr, LENGTH(ls_chg_dte_ocr) - 10 + 1))) = (SELECT input_month FROM parameters) 
    and acct_type = 'R' 
    and ord_typ = 'CONNECT' and order_rsn = 'RS'
)

--------------------------------- VOLUNTARY CHURN ---------------------------------------
,service_orders_flag AS (
SELECT  DATE_TRUNC('MONTH',DATE(completed_date)) AS month
        ,DATE(completed_date) AS end_date
        ,DATE(order_start_date) AS start_date
        ,cease_reason_code
        ,cease_reason_desc
        ,order_type
        ,CASE   
            WHEN cease_reason_desc = 'MIG COAX TO FIB' THEN 'Migracion'
            -- WHEN account_id in (SELECT sub_acct_no_ooi FROM "lcpr.sandbox.dev"."transactions_orderactivity" WHERE date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters) and acct_type = 'R' and ord_typ = 'V_DISCO' and disco_rsn_sbb != 'VL') then 'Migracion' --- disco_rsn_sbb should be replaced by ord_rsn as soon as the column is available
            WHEN cease_reason_desc = 'NON-PAY' THEN 'Involuntario'
            -- WHEN account_id in (SELECT sub_acct_no_ooi FROM "lcpr.sandbox.dev"."transactions_orderactivity" WHERE date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters) and acct_type = 'R' and ord_typ = 'NON PAY') then 'Involuntario'
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
    AND account_id not in (SELECT account_id FROM oa_transfers) --- We do not want to consider Transfers as Vol Churns
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
        ,A.fix_b_mes_num_rgus
        ,A.fix_e_fla_active
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
        ,fix_e_fla_active
        ,IF(ChurnType IS NOT NULL AND fix_e_fla_active = 1 AND fix_b_mes_num_rgus > NumRGUs_LastRecord,1,0) AS partial_churn
FROM voluntary_churners
)

,final_voluntary_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,IF(churn_account IS NOT NULL AND (fix_e_fla_active = 0 OR fix_e_fla_active IS NULL),'1. Fixed Voluntary Churner',NULL) AS fix_s_fla_churn_type
FROM voluntary_churners_adj
WHERE ChurnType IS NOT NULL AND partial_churn = 0 
)

-------------------------------- INVOLUNTARY CHURN --------------------------------------
,first_cust_record AS (
SELECT  DATE_TRUNC('MONTH',DATE_ADD('MONTH',1,DATE(dt))) AS mes
        ,fix_s_att_account
        ,case when fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM BOM_subsidized_customers) /*acp is not null*/ then 1 else 0 end as acp
        ,MIN(date(dt)) AS first_cust_record
        ,DATE_ADD('day',-1,MIN(date(dt))) AS prev_first_cust_record
FROM useful_fields
WHERE date(dt) = date_trunc('MONTH', DATE(dt)) + interval '1' MONTH - interval '1' day
GROUP BY 1,2,3
)

,last_cust_record AS (
SELECT  DATE_TRUNC('MONTH',DATE(dt)) AS mes
        ,fix_s_att_account
        ,case when fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM EOM_subsidized_customers) /*acp is not null*/ then 1 else 0 end as acp
        ,MAX(date(dt)) AS last_cust_record
        ,DATE_ADD('day',-1,MAX(date(dt))) AS prev_last_cust_record
        ,DATE_ADD('day',-2,MAX(date(dt))) AS prev_last_cust_record2
FROM useful_fields
GROUP BY 1,2,3
ORDER BY 1,2
)

,no_overdue AS (
SELECT  DATE_TRUNC('MONTH', DATE_ADD('MONTH',1, DATE(A.dt))) AS MES
        ,A.fix_s_att_account
        ,A.overdue
FROM useful_fields A INNER JOIN first_cust_record B ON A.fix_s_att_account = B.fix_s_att_account
WHERE (CAST(A.overdue as INT) < (SELECT overdue_days FROM parameters) OR A.fix_s_att_account IN (SELECT DISTINCT sub_acct_no_sbb FROM BOM_subsidized_customers)) /*(SELECT account_id FROM bom_subsidized_in_overdue))*/
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
    AND (CAST(A.overdue AS INTEGER) >= (SELECT overdue_days FROM parameters) AND A.fix_s_att_account NOT IN (SELECT DISTINCT sub_acct_no_sbb FROM EOM_subsidized_customers)) /*(SELECT account_id FROM eom_subsidized_in_overdue))*/
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
        ,CASE WHEN A.fix_s_att_account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS fix_s_fla_churn_type
FROM involuntary_net_churners A LEFT JOIN useful_fields B on A.fix_s_att_account = B.fix_s_att_account AND A.month = DATE_TRUNC('month',date(B.dt))
where last_overdue >= (SELECT overdue_days FROM parameters) AND B.fix_s_att_account NOT IN (SELECT DISTINCT sub_acct_no_sbb FROM EOM_subsidized_customers)
GROUP BY 1,2,4,3
)

,final_involuntary_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,fix_s_fla_churn_type
FROM involuntary_churners
WHERE fix_s_fla_churn_type = '2. Fixed Involuntary Churner'
)

,all_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,fix_s_fla_churn_type
FROM    (SELECT month,churn_account,fix_s_fla_churn_type FROM final_voluntary_churners A 
         UNION ALL
         SELECT month,churn_account,fix_s_fla_churn_type FROM final_involuntary_churners B
        )
)

,fixed_table_churn_flag AS(
SELECT  A.*
        ,CASE   WHEN B.churn_account IS NOT NULL AND fix_s_fla_main_movement = '6.Null last day' THEN '1. Fixed Churner'
                --WHEN B.churn_account IS NULL AND fix_s_fla_main_movement <> '6.Null last day' THEN '2. Fixed NonChurner'
                ELSE '2. Fixed NonChurner'
                    END AS fix_s_fla_ChurnFlag
        ,CASE   WHEN B.churn_account IS NOT NULL THEN fix_s_fla_churn_type
                    ELSE NULL END AS fix_s_fla_churn_type
FROM spin_movement_flag A LEFT JOIN all_churners B ON A.fix_s_att_account = B.churn_account AND A.fix_s_dim_month = B.month
)

------------------------------------ REJOINERS ------------------------------------------

,inactive_users AS (
SELECT  DISTINCT fix_s_dim_month AS exit_month
        ,fix_s_att_account AS account
        ,DATE_ADD('MONTH',1,date(fix_s_dim_month)) AS rejoiner_month
FROM customer_status
WHERE fix_b_fla_active = 1 AND fix_e_fla_active = 0
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
        ,IF(fix_s_fla_PRMonth = 1 AND fix_s_fla_main_movement = '5.Come Back to Life',1,0) AS fix_s_fla_Rejoiner
FROM fixed_table_churn_flag A LEFT JOIN fixed_rejoiner_month_population B
    ON A.fix_s_att_account = B.fix_s_att_account AND A.fix_s_dim_month = CAST(B.month AS DATE)
)

,invol_flag_SO AS (
SELECT  DISTINCT A.*
        ,IF(fix_s_fla_ChurnFlag = '2. Fixed NonChurner' AND fix_e_fla_active = 0,'Churner Gap',NULL) AS Gap
        ,IF(dx_type = 'Involuntario' AND fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_e_fla_active = 0,1,0) AS early_dx_flag
        ,IF(dx_type = 'Migracion' AND fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_e_fla_active = 0,1,0) AS migrt_flag
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
        ,IF(A.fix_b_dim_mix_code_adj IS NOT NULL AND B.sub_acct_no_sbb IS NOT NULL,'Churner0P',NULL) AS churn0p
FROM invol_flag_SO A LEFT JOIN prepaid_churners B 
    ON A.fix_s_dim_month = B.month AND A.fix_s_att_account = B.sub_acct_no_sbb
)

-- ,transfers AS (
-- SELECT account_id
-- FROM "lcpr.stage.prod"."so_hdr_lcpr"
-- WHERE TRIM(order_type) = 'RELOCATION/TRAN'
--     AND account_type = 'RES'
--     AND order_status = 'COMPLETE'
--     AND DATE_TRUNC('MONTH',DATE(dt)) = (SELECT input_month FROM parameters) 
-- )

,final_fixed_flags AS (
SELECT  fix_s_dim_month
        ,fix_s_att_account
        ,fix_b_fla_active
        ,IF(fix_s_fla_churn_type = '2. Fixed Involuntary Churner',0,fix_e_fla_active) AS fix_e_fla_active
        ,fix_s_att_contact_phone1
        ,if(fix_s_att_contact_phone2 > 100,fix_s_att_contact_phone2,NULL) AS fix_s_att_contact_phone2
        ,fix_b_dim_date
        ,fix_b_mes_outstage
        ,fix_b_dim_max_start
        ,fix_b_fla_tenure
        ,fix_b_mes_mrc
        ,fix_b_fla_tech_type
        ,fix_b_fla_fmc
        ,fix_b_mes_num_rgus
        ,fix_b_att_mix_name_adj
        ,fix_b_dim_mix_code_adj
        ,fix_b_fla_bb_rgu
        ,fix_b_fla_tv_rgu
        ,fix_b_fla_vo_rgu
        ,fix_b_dim_bb_code
        ,fix_b_dim_tv_code
        ,fix_b_dim_vo_code
        ,fix_b_fla_subsidized
        ,fix_b_att_BillCode
        ,fix_e_dim_date
        ,fix_e_mes_outstage
        ,fix_e_dim_max_start
        ,fix_e_fla_tenure
        ,fix_e_mes_mrc
        ,fix_e_fla_tech_type
        ,fix_e_fla_fmc
        ,IF(fix_s_fla_churn_type = '2. Fixed Involuntary Churner',0,fix_e_mes_num_rgus) AS fix_e_mes_num_rgus
        ,fix_e_att_mix_name_adj
        ,fix_e_fla_MixCodeAdj
        ,fix_e_dim_bb_rgu
        ,fix_e_dim_tv_rgu
        ,fix_e_dim_vo_rgu
        ,fix_e_dim_bb_code
        ,fix_e_dim_tv_code
        ,fix_e_dim_vo_code
        ,fix_e_fla_subsidized
        ,fix_e_att_BillCode
        ,IF(fix_s_fla_churn_type = '2. Fixed Involuntary Churner','6.Null last day',IF(fix_s_att_account IN (SELECT distinct account_id FROM /*transfers*/ oa_transfer_adds) AND fix_s_fla_main_movement IN('4.New Customer','5.Come Back to Life'),'7.Transfer Adds',fix_s_fla_main_movement)) AS fix_s_fla_main_movement
        ,IF(fix_s_fla_churn_type IS NOT NULL,'3. No Spin',fix_s_fla_spin_movement) AS fix_s_fla_spin_movement
        ,CASE   
            WHEN (early_dx_flag + migrt_flag) >= 1 THEN '1. Fixed Churner'
            WHEN fix_s_fla_ChurnFlag = '2. Fixed NonChurner' AND fix_b_fla_active = 1 AND fix_e_fla_active = 0 /*AND churn0p = 'Churner0P'*/ THEN '1. Fixed Churner'
            WHEN fix_s_fla_churn_type IS NOT NULL THEN '1. Fixed Churner'
        ELSE fix_s_fla_ChurnFlag END AS fix_s_fla_ChurnFlag
        ,CASE   
            WHEN (fix_s_fla_main_movement = '6.Null last day' AND (fix_s_fla_churn_type IS NULL OR fix_s_att_account in (SELECT account_id FROM oa_transfers)) ) THEN '3. Fixed Transfer'
            WHEN early_dx_flag = 1 THEN '2. Fixed Involuntary Churner'
            WHEN migrt_flag = 1 THEN '1. Fixed Voluntary Churner'
            WHEN fix_s_fla_ChurnFlag = '1. Fixed Churner' AND fix_b_fla_active = 1 AND fix_e_fla_active = 0 AND churn0p = 'Churner0P' THEN '4. Fixed 0P Churner'
        ELSE fix_s_fla_churn_type END AS fix_s_fla_churn_type
        ,CASE   
            WHEN (fix_s_fla_main_movement = '6.Null last day' AND (fix_s_fla_churn_type IS NULL OR fix_s_att_account in (SELECT account_id FROM oa_transfers)) ) THEN 'Transfer'
            WHEN early_dx_flag = 1 THEN 'Early Dx'
            WHEN migrt_flag = 1 THEN 'Incomplete CST'
            WHEN fix_s_fla_churn_type = '1. Fixed Voluntary Churner' THEN 'Voluntary'
            WHEN fix_s_fla_churn_type = '2. Fixed Involuntary Churner' THEN 'Involuntary'
            WHEN fix_s_fla_ChurnFlag = '2. Fixed NonChurner' AND fix_b_fla_active
                = 1 AND fix_e_fla_active = 0 AND churn0p = 'Churner0P' THEN '0P Churner'
        END AS fix_s_fla_final_churn
        ,fix_s_fla_Rejoiner
FROM prepaid_churner_flag
)

SELECT *
FROM final_fixed_flags
WHERE fix_s_dim_month = (SELECT input_month FROM parameters)
    AND fix_b_fla_active + fix_e_fla_active >= 1


--- --- --- ### ### Quick check

-- select  fix_s_dim_month
--         ,fix_b_fla_active
--         ,fix_e_fla_active
--         ,fix_s_fla_Main_Movement
--         ,fix_s_fla_spin_movement
--         ,fix_s_fla_ChurnFlag
--         ,fix_s_fla_churn_type
--         ,count(distinct fix_s_att_account) as accounts
--         ,sum(fix_b_mes_num_RGUS) as BOM_RGUS
--         ,sum(fix_e_mes_num_RGUS) as EOM_RGUS
-- FROM final_fixed_flags
-- WHERE fix_s_dim_month = (SELECT input_month FROM parameters)
-- GROUP BY 1,2,3,4,5,6,7
-- ORDER BY 1,4,5,6,7
