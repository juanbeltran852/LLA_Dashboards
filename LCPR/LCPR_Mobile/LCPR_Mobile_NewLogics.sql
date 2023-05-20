-----------------------------------------------------------------------------------------
-------------------------- LCPR POSTPAID TABLE - V2 -------------------------------------
-----------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_postpaid_table_feb_apr14" AS

WITH 

parameters AS (
--> Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-04-01')) AS input_month
        ,85 as overdue_days
)

, cust_mstr_adj as (
SELECT
    *, 
    first_value(date(subsrptn_actvtn_dt)) over (partition by subsrptn_id order by dt asc) as activation_dt
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) between ((SELECT input_month FROM parameters) - interval '3' month) and (SELECT input_month FROM parameters)
)

,BOM_active_base AS (
SELECT  subsrptn_id AS account
        ,cust_id AS parent_account
        ,(date(dt) + interval '1' month - interval '1' day) AS mob_b_dim_date
        ,DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) AS mob_b_mes_TenureDays
        ,date(activation_dt) AS mob_b_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid-Tenure'        
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late-Tenure'
                    ELSE NULL END AS mob_b_fla_Tenure
        ,null AS mob_b_mes_MRC
        ,1 AS mob_b_mes_numRGUS
FROM cust_mstr_adj
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
        ,date(activation_dt) AS mob_e_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid-Tenure'        
                WHEN DATE_DIFF('day',date(subsrptn_actvtn_dt),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late-Tenure'
                    ELSE NULL END AS mob_e_fla_Tenure
        ,null AS mob_e_mes_MRC
        ,1 AS mob_e_mes_numRGUS
FROM cust_mstr_adj
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

,MRC_ext_calculus AS (
SELECT  date(dt) AS dt
        ,acct_nbr AS parent
        ,sum("mrc/account") AS MRC_per_parent
FROM "lcpr.stage.dev"."lcpr_mob_postpaid_mrc"
WHERE DATE(dt) BETWEEN (SELECT input_month FROM parameters) - interval '1' month AND (SELECT input_month FROM parameters)
GROUP BY 1,2
)

,fixed_mrc AS (
SELECT  A.mob_s_att_ParentAccount
        ,B.MRC_per_parent AS BOM_MRC
        ,C.MRC_per_parent AS EOM_MRC
        ,sum(A.mob_b_att_active) as bom_active
        ,sum(A.mob_e_att_active) as eom_active
        ,CASE WHEN sum(A.mob_b_att_active) = 0 THEN Null else (B.MRC_per_parent/sum(A.mob_b_att_active)) end as BOM_MRC_per_subs
        ,CASE WHEN sum(A.mob_e_att_active) = 0 THEN Null else (C.MRC_per_parent/sum(A.mob_e_att_active)) end as EOM_MRC_per_subs
FROM customer_status A
LEFT JOIN (SELECT * FROM MRC_ext_calculus WHERE dt = (SELECT input_month FROM parameters) - interval '1' month) B
ON A.mob_s_att_ParentAccount = B.parent
LEFT JOIN (SELECT * FROM MRC_ext_calculus WHERE dt = (SELECT input_month FROM parameters)) C
ON A.mob_s_att_ParentAccount = C.parent
GROUP BY 1,2,3
)

,customer_status_2 AS (
SELECT  A.mob_s_dim_month
        ,A.mob_s_att_account
        ,A.mob_s_att_ParentAccount
        ,A.mob_s_att_MobileType
        ,A.mob_b_att_active
        ,A.mob_e_att_active
        ,A.mob_b_dim_date
        ,A.mob_b_mes_TenureDays
        ,A.mob_b_att_MaxStart
        ,A.mob_b_fla_Tenure
        ,IF(A.mob_s_att_ParentAccount = B.mob_s_att_ParentAccount,B.BOM_MRC_per_subs,A.mob_b_mes_MRC) AS mob_b_mes_MRC
        ,A.mob_b_mes_numRGUS
        ,A.mob_e_dim_date
        ,A.mob_e_mes_TenureDays
        ,A.mob_e_att_MaxStart
        ,A.mob_e_fla_Tenure
        ,IF(A.mob_s_att_ParentAccount = B.mob_s_att_ParentAccount,B.EOM_MRC_per_subs,A.mob_e_mes_MRC) AS mob_e_mes_MRC
        ,A.mob_e_mes_numRGUS
FROM customer_status A LEFT JOIN fixed_mrc B 
ON A.mob_s_att_ParentAccount = B.mob_s_att_ParentAccount
)

, newcust_candidates as (
SELECT
   A.mob_s_att_account as newcust_candidate_flag
FROM customer_status_2 A
WHERE
    A.mob_s_att_account not in (
    SELECT 
        subsrptn_id 
    FROM cust_mstr_adj
    WHERE date(dt) between ((SELECT input_month FROM parameters) - interval '6' month) and ((SELECT input_month FROM parameters) - interval '1' month)
        --> Flags utilizadas para clasificar a los usuarios residenciales. Input de Juan C. Vega
        AND cust_sts = 'O'
        AND acct_type_cd = 'I'
        AND rgn_nm <> 'VI'
        AND subsrptn_sts = 'A' 
        )
)

,main_movement_flag AS(
SELECT  *
        ,CASE   WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) = 0 THEN '1.SameRGUs' 
                WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) > 0 THEN '2.Upsell'
                WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) < 0 THEN '3.Downsell'
                -- WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND DATE_TRUNC('MONTH',mob_e_att_MaxStart) =  mob_s_dim_month) THEN '4.New Customer'
                -- WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND DATE_TRUNC('MONTH',mob_e_att_MaxStart) <> mob_s_dim_month) THEN '5.Come Back to Life'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND newcust_candidate_flag is not null) THEN '4.New Customer'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND newcust_candidate_flag is null) THEN '5.Come Back to Life'
                WHEN (mob_b_mes_numRGUS > 0 AND mob_e_mes_numRGUS IS NULL) THEN '6.Null last day'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS IS NULL) THEN '7.Always null'
                    END AS mob_s_fla_MainMovement
FROM customer_status_2 A
LEFT JOIN newcust_candidates B
    ON A.mob_s_att_account = B.newcust_candidate_flag
)

,spin_movement_flag AS(
SELECT  *
        ,ROUND((IF(mob_e_mes_MRC IS NULL,0,mob_e_mes_MRC) - IF(mob_b_mes_MRC IS NULL,0,mob_b_mes_MRC)),0) AS mob_s_mes_MRCdiff
        ,CASE   WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (IF(mob_e_mes_MRC IS NULL,0,mob_e_mes_MRC) - IF(mob_b_mes_MRC IS NULL,0,mob_b_mes_MRC)) = 0 THEN '1.Same'
                WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (IF(mob_e_mes_MRC IS NULL,0,mob_e_mes_MRC) - IF(mob_b_mes_MRC IS NULL,0,mob_b_mes_MRC)) > 0 THEN '2.Upspin'
                WHEN mob_s_fla_MainMovement = '1.SameRGUs' AND (IF(mob_e_mes_MRC IS NULL,0,mob_e_mes_MRC) - IF(mob_b_mes_MRC IS NULL,0,mob_b_mes_MRC)) < 0 THEN '3.Downspin'
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
FROM cust_mstr_adj
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
FROM cust_mstr_adj
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
        ,IF(CAST(mob_b_mes_MRC AS VARCHAR) = 'NaN',NULL,mob_b_mes_MRC) AS mob_b_mes_MRC
        ,mob_b_mes_numRGUS
        ,mob_e_dim_date
        ,mob_e_mes_TenureDays
        ,mob_e_att_MaxStart
        ,mob_e_fla_Tenure
        ,IF (mob_e_mes_MRC IS NULL, null,mob_e_mes_MRC) AS mob_e_mes_MRC
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
WHERE 
    mob_b_att_active + mob_e_att_active >= 1
    -- and mob_s_att_account = 7872310633

-- SELECT count(distinct newcust_candidate_flag)
-- FROM newcust_candidates
-- WHERE 
    -- newcust_candidate_flag = 7872310633
