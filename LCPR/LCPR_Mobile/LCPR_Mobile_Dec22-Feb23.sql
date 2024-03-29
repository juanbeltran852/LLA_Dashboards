-----------------------------------------------------------------------------------------
-------------------------- LCPR POSTPAID TABLE - V1 -------------------------------------
-----------------------------------------------------------------------------------------
--- Comment (2023-07-20): Use this code ofr obtaning Mobile results for Feb23 and before ---

--CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_postpaid_table_jan_mar15" AS

WITH 

parameters AS (
--> Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-02-01')) AS input_month
        ,85 as overdue_days
)

,BOM_active_base AS (
SELECT  subsrptn_id AS account
        ,cust_id AS parent_account
        ,(date(dt) + interval '1' month - interval '1' day) AS mob_b_dim_date
        ,DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) AS mob_b_mes_TenureDays
        ,date(subsrptn_actvtn_dt) AS mob_b_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early Tenure'
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid Tenure'        
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late Tenure'
                    ELSE NULL END AS mob_b_fla_Tenure
        ,indiv_inslm_amt AS mob_b_mes_MRC
        ,1 AS mob_b_mes_numRGUS
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month
    --> Flags utilizadas para clasificar a los usuarios residenciales. Input de Juan C. Vega
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
)

,EOM_active_base AS (
SELECT  subsrptn_id as account
        ,cust_id AS parent_account
        ,(date(dt) + interval '1' month - interval '1' day) AS mob_e_dim_date
        ,DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) AS mob_e_mes_TenureDays
        ,date(subsrptn_actvtn_dt) AS mob_e_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early Tenure'
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid Tenure'        
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late Tenure'
                    ELSE NULL END AS mob_e_fla_Tenure
        ,indiv_inslm_amt AS mob_e_mes_MRC
        ,1 AS mob_e_mes_numRGUS
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters)
        --> Flags utilizadas para clasificar a los usuarios residenciales. Input de Juan C. Vega
        AND cust_sts = 'O'
        AND acct_type_cd = 'I'
        AND rgn_nm <> 'VI'
        AND subsrptn_sts = 'A'
)

,customer_status AS (
SELECT  (SELECT input_month FROM parameters) AS mob_s_dim_month
        ,CASE   WHEN (A.account IS NOT NULL AND B.account IS NOT NULL) OR (A.account IS NOT NULL AND B.account IS NULL) THEN A.account
                WHEN (A.account IS NULL AND B.account IS NOT NULL) THEN B.account
                    END AS mob_s_att_account
        ,CASE   WHEN (A.parent_account IS NOT NULL AND B.parent_account IS NOT NULL) OR (A.parent_account IS NOT NULL AND B.parent_account IS NULL) THEN A.parent_account
                WHEN (A.parent_account IS NULL AND B.parent_account IS NOT NULL) THEN B.parent_account
                    ELSE NULL END AS mob_s_att_ParentAccount
        ,'Postpaid' AS mob_s_att_MobileType
        ,IF(A.account IS NOT NULL,1,0) AS mob_b_att_active
        ,IF(B.account IS NOT NULL,1,0) AS mob_e_att_active
        ,mob_b_dim_date
        ,mob_b_mes_TenureDays
        ,mob_b_att_MaxStart
        ,mob_b_fla_Tenure
        ,mob_b_mes_MRC
        ,mob_b_mes_numRGUS
        ,mob_e_dim_date
        ,mob_e_mes_TenureDays
        ,mob_e_att_MaxStart
        ,mob_e_fla_Tenure
        ,mob_e_mes_MRC
        ,mob_e_mes_numRGUS
FROM BOM_active_base A FULL OUTER JOIN EOM_active_base B
    ON A.account = B.account
)

,main_movement_flag AS(
SELECT  *
        ,CASE   WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) = 0 THEN '1.SameRGUs' 
                WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) > 0 THEN '2.Upsell'
                WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) < 0 THEN '3.Downsell'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND DATE_TRUNC('MONTH',mob_e_att_MaxStart) =  mob_s_dim_month) THEN '4.New Customer'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND DATE_TRUNC('MONTH',mob_e_att_MaxStart) <> mob_s_dim_month) THEN '5.Come Back to Life'
                WHEN (mob_b_mes_numRGUS > 0 AND mob_e_mes_numRGUS IS NULL) THEN '6.Null last day'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS IS NULL) THEN '7.Always null'
                    END AS mob_s_fla_MainMovement
FROM customer_status
)

,spin_movement_flag AS(
SELECT  *
        ,ROUND((mob_e_mes_MRC - mob_b_mes_MRC),0) AS mob_s_mes_MRCdiff
        ,CASE   WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (mob_e_mes_MRC - mob_b_mes_MRC) = 0 THEN '1.Same'
                WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (mob_e_mes_MRC - mob_b_mes_MRC) > 0 THEN '2.Upspin'
                WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (mob_e_mes_MRC - mob_b_mes_MRC) < 0 THEN '3.Downspin'
                    ELSE '4.NoSpin' END AS mob_s_fla_SpinMovement
FROM main_movement_flag 
)

,disconnections AS (
SELECT  DATE_TRUNC('MONTH',DATE(dt)) AS month
        ,subsrptn_id AS churn_account
        ,date(substr(subsrptn_sts_dt,1,10)) AS disconnection_date
        ,acct_sts_rsn_desc
        ,IF((lower(acct_sts_rsn_desc) LIKE '%no%pay%' 
                or lower(acct_sts_rsn_desc) LIKE '%no%use%'
                or lower(acct_sts_rsn_desc) LIKE '%fraud%'
                or lower(acct_sts_rsn_desc) LIKE '%off%net%'
                or lower(acct_sts_rsn_desc) LIKE '%pay%def%'
                or lower(acct_sts_rsn_desc) LIKE '%lost%equip%'
                or lower(acct_sts_rsn_desc) LIKE '%tele%conv%'
                or lower(acct_sts_rsn_desc) LIKE '%cont%acce%req%'
                or lower(acct_sts_rsn_desc) LIKE '%proce%'
                or lower(lst_susp_rsn_desc) LIKE '%no%pay%'
                or lower(lst_susp_rsn_desc) LIKE '%no%use%'
                or lower(lst_susp_rsn_desc) LIKE '%fraud%'
                or lower(lst_susp_rsn_desc) LIKE '%off%net%'
                or lower(lst_susp_rsn_desc) LIKE '%pay%def%'
                or lower(lst_susp_rsn_desc) LIKE '%lost%equip%'
                or lower(lst_susp_rsn_desc) LIKE '%tele%conv%'
                --or lower(lst_susp_rsn_desc) LIKE '%cont%acce%req%'
                or lower(lst_susp_rsn_desc) LIKE '%proce%'),'Involuntary','Voluntary') AS churn_type
FROM "lcpr.stage.dev"."tbl_pstpd_cust_cxl_incr_data"
WHERE DATE(dt) = (SELECT input_month FROM parameters)
        --AND cust_sts = 'O'
        AND acct_type_cd = 'I'
        AND rgn_nm <> 'VI'
        --AND subsrptn_sts <> 'A'
    AND subsrptn_id NOT IN (SELECT DISTINCT subsrptn_id
                        FROM "lcpr.stage.dev"."tbl_pstpd_cust_cxl_incr_data"
                        WHERE DATE(dt) = (SELECT input_month FROM parameters) - interval '1' month
                            --AND cust_sts = 'O'
                            AND acct_type_cd = 'I'
                            AND rgn_nm <> 'VI'
                            --AND subsrptn_sts = 'A'
                        )
)

,all_churners AS (
SELECT  month
        ,churn_account
        ,churn_type
FROM    (SELECT *
                ,row_number() over (PARTITION BY churn_account order by disconnection_date DESC) as row_num
        FROM disconnections)
--where row_num = 1
)

,mobile_table_churn_flag AS(
SELECT  A.*
        ,CASE   WHEN B.churn_account IS NOT NULL THEN '1. Mobile Churner'
                WHEN B.churn_account IS NULL THEN '2. Mobile NonChurner'
                    END AS mob_s_fla_ChurnFlag
        ,CASE   WHEN B.churn_type = 'Involuntary' THEN '2. Mobile Involuntary Churner'
                WHEN B.churn_type = 'Voluntary' THEN '1. Mobile Voluntary Churner'
                    ELSE NULL END AS mob_s_fla_ChurnType
FROM spin_movement_flag A LEFT JOIN all_churners B ON A.mob_s_att_account = B.churn_account AND A.mob_s_dim_month = B.month
)

,rejoiner_candidates AS (
SELECT  subsrptn_id AS rejoiner_account
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '2' month
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
    AND subsrptn_id NOT IN (SELECT DISTINCT subsrptn_id
                            FROM "lcpr.stage.dev"."tbl_pstpd_cust_cxl_incr_data"
                            WHERE DATE(dt) = (SELECT input_month FROM parameters) - interval '1' month
                            AND cust_sts = 'O'
                            AND acct_type_cd = 'I'
                            AND rgn_nm <> 'VI'
                            AND subsrptn_sts = 'A')
)

,cleaning as (
SELECT  subsrptn_id AS account
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
    AND flg_susp_curr_mo = 0
    AND DATE_TRUNC('MONTH',DATE(IF(LENGTH(TRIM(lst_susp_dt)) = 10,TRIM(lst_susp_dt),'1900-01-01'))) <> (SELECT input_month FROM parameters) - interval '1' month
)

,full_flags AS (
SELECT  mob_s_dim_month
        ,mob_s_att_account
        ,mob_s_att_ParentAccount
        ,mob_s_att_MobileType
        ,IF(mob_s_att_account NOT IN (SELECT DISTINCT account FROM cleaning) AND mob_s_fla_MainMovement = '6.Null last day',0,mob_b_att_active) AS mob_b_att_active
        ,IF(mob_s_fla_ChurnFlag = '1. Mobile Churner',0,mob_e_att_active) AS mob_e_att_active
        ,mob_b_dim_date
        ,mob_b_mes_TenureDays
        ,mob_b_att_MaxStart
        ,mob_b_fla_Tenure
        ,mob_b_mes_MRC
        ,mob_b_mes_numRGUS
        ,mob_e_dim_date
        ,mob_e_mes_TenureDays
        ,mob_e_att_MaxStart
        ,mob_e_fla_Tenure
        ,mob_e_mes_MRC
        ,IF(mob_s_fla_ChurnFlag = '1. Mobile Churner',0,mob_e_mes_numRGUS) AS mob_e_mes_numRGUS
        ,IF(mob_s_fla_ChurnFlag = '1. Mobile Churner','6.Null last day',mob_s_fla_MainMovement) AS mob_s_fla_MainMovement
        ,mob_s_mes_MRCdiff
        ,IF(mob_s_fla_ChurnFlag = '1. Mobile Churner','4.NoSpin',mob_s_fla_SpinMovement) AS mob_s_fla_SpinMovement
        ,IF(mob_s_fla_MainMovement = '6.Null last day' AND mob_s_fla_ChurnFlag = '2. Mobile NonChurner','1. Mobile Churner',mob_s_fla_ChurnFlag) AS mob_s_fla_ChurnFlag
        ,IF(mob_s_fla_MainMovement = '6.Null last day' AND mob_s_fla_ChurnType IS NULL,'1. Mobile Voluntary Churner',mob_s_fla_ChurnType) AS mob_s_fla_ChurnType
        ,IF(mob_b_att_active = 0 AND mob_e_att_active = 1 AND mob_s_att_account IN (SELECT DISTINCT rejoiner_account FROM rejoiner_candidates),1,0) AS mob_s_fla_Rejoiner
FROM mobile_table_churn_flag
)

SELECT *
FROM full_flags
WHERE mob_b_att_active + mob_e_att_active >= 1

--- ### ### Quick check
-- SELECT
--     mob_s_dim_month, 
--     mob_b_att_active, 
--     mob_e_att_active, 
--     mob_s_fla_MainMovement, 
--     mob_s_fla_SpinMovement, 
--     mob_s_fla_ChurnFlag, 
--     mob_s_fla_ChurnType, 
--     count(distinct mob_s_att_account) as accounts, 
--     count(distinct case when mob_b_att_active = 1 then mob_s_att_account else null end) as EOM_RGUs, 
--     count(distinct case when mob_e_att_active = 1 then mob_s_att_account else null end) as BOM_RGUs
-- FROM full_flags
-- WHERE
--     mob_b_att_active + mob_e_att_active >= 1
--     -- and mob_s_att_account not in (SELECT mob_s_att_account FROM extra_users)
-- GROUP BY 1, 2, 3, 4, 5, 6, 7
-- ORDER BY 4 asc, 5 asc, 6 asc, 7 asc
