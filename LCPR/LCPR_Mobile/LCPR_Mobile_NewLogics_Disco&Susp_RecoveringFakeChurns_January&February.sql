-----------------------------------------------------------------------------------------
--------------------------- LCPR MOBILE TABLE - V1 --------------------------------------
-----------------------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_mob_feb2023_adj" AS

WITH 

parameters AS (
--> Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-02-01')) AS input_month
        ,85 as overdue_days
)

, cust_mstr_adj_pre as (
SELECT
    *, 
    first_value(date(subsrptn_actvtn_dt)) over (partition by subsrptn_id order by dt asc) as activation_dt, 
    1 as row_nm
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) between ((SELECT input_month FROM parameters) - interval '6' month) and (SELECT input_month FROM parameters)
)

, fake_churns_pre as (
SELECT
    DATE_TRUNC('MONTH',DATE(dt)) AS month,
    subsrptn_id AS fake_churn_id, 
    acct_sts_rsn_desc, 
    case when 
        lower(acct_sts_rsn_desc) like '%contract%accepted%'
        or lower(acct_sts_rsn_desc) like '%portin%'
        or lower(acct_sts_rsn_desc) like '%ctn%activation%'
        or lower(acct_sts_rsn_desc) like '%per%cust%req%'
        or lower(acct_sts_rsn_desc) like '%reduced%rate%suspend%'
        or lower(acct_sts_rsn_desc) like '%""%'
        or lower(lst_susp_rsn_desc) like '%""%'
    then subsrptn_id else null end as invalid_reasons_flag, 
    first_value(date(subsrptn_sts_dt)) over (partition by subsrptn_id order by dt desc) as last_disco_dt, 
    row_number() over (partition by subsrptn_id order by date(subsrptn_sts_dt) desc) as row_nm
FROM "lcpr.stage.dev"."tbl_pstpd_cust_cxl_incr_data"
WHERE 
    date_trunc('month', date(dt)) between ((SELECT input_month FROM parameters) - interval '6' month) and ((SELECT input_month FROM parameters) - interval '1' month)
)

, fake_churns as (
SELECT
    month, 
    fake_churn_id,
    acct_sts_rsn_desc, 
    invalid_reasons_flag, 
    last_disco_dt
FROM fake_churns_pre
WHERE
    row_nm = 1
    and 
    invalid_reasons_flag is not null
)

, missing_accounts as (
SELECT
    *, 
    first_value(date(subsrptn_actvtn_dt)) over (partition by subsrptn_id order by dt asc) as activation_dt,
    row_number() over (partition by subsrptn_id order by dt desc) as row_nm, 
    (SELECT input_month FROM parameters) as dt_2
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE 
    date(dt) between ((SELECT input_month FROM parameters) - interval '6' month) and ((SELECT input_month FROM parameters) - interval '1' month)
    and subsrptn_id in (SELECT fake_churn_id FROM fake_churns)
)

, cust_mstr_adj as (
SELECT
    *
FROM missing_accounts
WHERE row_nm = 1

UNION ALL

SELECT
    *, 
    date(dt) as dt_2
FROM cust_mstr_adj_pre
WHERE
    subsrptn_id not in (SELECT fake_churn_id FROM fake_churns)
)

,BOM_active_base AS (
SELECT  subsrptn_id AS account
        ,cust_id AS parent_account
        ,(date(dt) + interval '1' month - interval '1' day) AS mob_b_dim_date
        ,DATE_DIFF('day',date(activation_dt),(date(dt) + interval '1' month - interval '1' day)) AS mob_b_mes_TenureDays
        ,date(activation_dt) AS mob_b_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(activation_dt),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('day',date(activation_dt),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid-Tenure'        
                WHEN DATE_DIFF('day',date(activation_dt),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late-Tenure'
                    ELSE NULL END AS mob_b_fla_Tenure
        ,indiv_inslm_amt AS mob_b_mes_MRC
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
        ,DATE_DIFF('day',date(activation_dt),(date(dt) + interval '1' month - interval '1' day)) AS mob_e_mes_TenureDays
        ,date(activation_dt) AS mob_e_att_MaxStart
        ,CASE   WHEN DATE_DIFF('day',date(activation_dt),(date(dt) + interval '1' month - interval '1' day)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('day',date(activation_dt),(date(dt) + interval '1' month - interval '1' day)) <= 360 THEN 'Mid-Tenure'        
                WHEN DATE_DIFF('day',date(activation_dt),(date(dt) + interval '1' month - interval '1' day)) >  360 THEN 'Late-Tenure'
                    ELSE NULL END AS mob_e_fla_Tenure
        ,indiv_inslm_amt AS mob_e_mes_MRC
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
        ,case 
            when B.account in (SELECT subsrptn_id FROM missing_accounts) then 1
            when A.account is not null then 1 
        else 0 end AS mob_b_att_active
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

, newcust_candidates as (
SELECT
   A.mob_s_att_account as newcust_candidate_flag
FROM customer_status A
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
        ,CASE   WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) = 0 or mob_s_att_account in (SELECT fake_churn_id FROM fake_churns) THEN '1.SameRGUs'
                WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) > 0 THEN '2.Upsell'
                WHEN (mob_e_mes_numRGUS - mob_b_mes_numRGUS) < 0 THEN '3.Downsell'
                -- WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND DATE_TRUNC('MONTH',mob_e_att_MaxStart) =  mob_s_dim_month) THEN '4.New Customer'
                -- WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND DATE_TRUNC('MONTH',mob_e_att_MaxStart) <> mob_s_dim_month) THEN '5.Come Back to Life'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND newcust_candidate_flag is not null) and mob_s_att_account not in (SELECT fake_churn_id FROM fake_churns) THEN '4.New Customer'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS > 0 AND newcust_candidate_flag is null) and mob_s_att_account not in (SELECT fake_churn_id FROM fake_churns) THEN '5.Come Back to Life'
                WHEN (mob_b_mes_numRGUS > 0 AND mob_e_mes_numRGUS IS NULL) THEN '6.Null last day'
                WHEN (mob_b_mes_numRGUS IS NULL AND mob_e_mes_numRGUS IS NULL) THEN '7.Always null'
                    END AS mob_s_fla_MainMovement
FROM customer_status A
LEFT JOIN newcust_candidates B
    ON A.mob_s_att_account = B.newcust_candidate_flag
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
    -- AND subsrptn_id NOT IN (SELECT DISTINCT subsrptn_id
    --                     FROM "lcpr.stage.dev"."tbl_pstpd_cust_cxl_incr_data"
    --                     WHERE DATE(dt) = (SELECT input_month FROM parameters) - interval '1' month
    --                         --AND cust_sts = 'O'
    --                         AND acct_type_cd = 'I'
    --                         AND rgn_nm <> 'VI'
    --                         --AND subsrptn_sts = 'A'
    --                     )
    and lower(acct_sts_rsn_desc) not like '%contract%accepted%'
    and lower(acct_sts_rsn_desc) not like '%portin%'
    and lower(acct_sts_rsn_desc) not like '%ctn%activation%'
    and lower(acct_sts_rsn_desc) not like '%per%cust%req%'
    and lower(acct_sts_rsn_desc) not like '%reduced%rate%suspend%'
    and lower(acct_sts_rsn_desc) not like '%""%'
    and lower(lst_susp_rsn_desc) not like '%""%'
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

,full_flags AS (
SELECT  mob_s_dim_month
        ,mob_s_att_account
        ,mob_s_att_ParentAccount
        ,mob_b_att_active
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
        ,mob_s_fla_SpinMovement
        ,mob_s_fla_ChurnFlag
        ,mob_s_fla_ChurnType
        ,IF(mob_b_att_active = 0 AND mob_e_att_active = 1 AND mob_s_att_account IN (SELECT DISTINCT rejoiner_account FROM rejoiner_candidates),1,0) AS mob_s_fla_Rejoiner
FROM mobile_table_churn_flag
)

, extra_users as (
SELECT
    mob_s_att_account
FROM full_flags
WHERE mob_e_mes_TenureDays < 0
    
)

-- SELECT *
-- FROM full_flags
-- WHERE 
--     mob_b_att_active + mob_e_att_active >= 1
    -- and mob_s_att_account = 7876004427

SELECT
    mob_s_dim_month, 
    mob_b_att_active, 
    mob_e_att_active, 
    mob_s_fla_MainMovement, 
    mob_s_fla_SpinMovement, 
    mob_s_fla_ChurnFlag, 
    mob_s_fla_ChurnType, 
    count(distinct mob_s_att_account) as accounts, 
    count(distinct case when mob_b_att_active = 1 then mob_s_att_account else null end) as EOM_RGUs, 
    count(distinct case when mob_e_att_active = 1 then mob_s_att_account else null end) as BOM_RGUs
FROM full_flags
WHERE
    mob_b_att_active + mob_e_att_active >= 1
    -- and mob_s_att_account not in (SELECT mob_s_att_account FROM extra_users)
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 4 asc, 5 asc, 6 asc, 7 asc
