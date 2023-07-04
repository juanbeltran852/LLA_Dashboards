--------------------------------------------------------------------------------
-------------------------- CWP - SALES QUALITY MOBILE --------------------------
--------------------------------------------------------------------------------

WITH

parameters as (
SELECT 
    date('2023-05-01') as input_month, 
    date_trunc('month', date('2023-06-01')) as current_month
),

--------------------------------------------------------------------------------
---------------------------------- Gross Adds ------------------------------
--------------------------------------------------------------------------------

gross_adds AS ( --Tabla Ventas que sale de gross adds
SELECT 
    TRIM(CAST(gross.account AS VARCHAR)) AS accountno, ------- Arround 1k of records do have their 'account' column in null
    gross.service AS serviceno,
    date(DATE_PARSE(TRIM(gross.date),'%m/%d/%Y')) AS sell_date, --Format example 05/27/2023
    gross.channel_resumen AS sell_channel,
    gross.procedencia AS procedencia,
    gross.agent_acc_code AS agent_acc_code,
    gross.plan_name AS plan_name
FROM "db-stage-prod"."gross_ads_movil_b2c_newversion" gross
WHERE
    date_trunc('month', DATE_PARSE(TRIM(gross.date),'%m/%d/%Y')) = (SELECT input_month FROM parameters)
-- LIMIT 1000
), 

--- Info early clients

--------------------------------------------------------------------------------
---------------------------------- Postpaid DNA -------------------------------
--------------------------------------------------------------------------------

useful_dna as (
SELECT
    -- billableaccountno as accountno,
    distinct serviceno as serviceno_dna,
    accountno as accountno_dna,
    first_value(date(dt)) over (partition by trim(cast(billableaccountno as varchar)) order by dt asc) as first_dt_user, --- Can be used as installation_date?
    date(fi_bill_dt_m0) as fi_bill_dt_m0,
    date(fi_bill_due_dt_m0) as fi_bill_due_dt_m0,
    cast(total_mrc_d as double) as total_mrc_d,
    cast(tot_inv_mo as double) AS tot_inv_mo,
    account_status as account_status,
    category as category, 
    first_value(province) over (partition by serviceno order by dt desc) as province, 
    first_value(district) over (partition by serviceno order by dt desc) as district,
    date(dt) as dt
FROM "db-analytics-prod"."tbl_postpaid_cwp"
WHERE
    account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
    and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters) -------- Why should I consider a time range between the input month and 4 months ahead?
    -- and total_mrc_d not in ('CORP 900')
    -- and tot_inv_mo not in ('CORP 900')
),

info_gross as (
SELECT
    distinct A.serviceno, 
    A.accountno, 
    A.sell_date, 
    first_value(A.sell_channel) over (partition by A.serviceno order by A.sell_channel) as sell_channel,
    first_value(A.procedencia) over (partition by A.serviceno order by A.procedencia) as procedencia,
    first_value(A.agent_acc_code) over (partition by A.serviceno order by A.agent_acc_code) as agent_acc_code,
    A.plan_name, 
    B.first_dt_user, 
    B.total_mrc_d,
    B.category, 
    B.province, 
    B.district, 
    case when B.fi_bill_dt_m0 is null then B.first_dt_user else date(fi_bill_dt_m0) end as first_bill_created_dt
FROM gross_adds A
INNER JOIN useful_dna B
    ON cast(A.serviceno as varchar) = cast(B.serviceno_dna as varchar)
-- WHERE date(B.dt) = (SELECT input_month FROM parameters)
),

--------------------------------------------------------------------------------
------------------------------------- PAYMENTS -----------------------------------
--------------------------------------------------------------------------------

info_pagos as (
SELECT
    distinct cast(A.serviceno as varchar) as serviceno_pagos, 
    cast(A.accountno as varchar) as accountno_pagos, 
    A.first_bill_created_dt, 
    round(SUM(TRY_CAST(payment_amt_local AS DOUBLE)), 2) AS pmnt_sell_month_ammnt,
    round(sum(cast(payment_amt_local as double)), 2) as total_payments_in_3_months, 
    round(sum(case when date_diff('day', date(A.first_bill_created_dt), date(B.dt)) < 30 then cast(B.payment_amt_local as double) else null end), 2) as total_payments_30_days,
    round(sum(case when date_diff('day', date(A.first_bill_created_dt), date(B.dt)) < 60 then cast(B.payment_amt_local as double) else null end), 2) as total_payments_60_days,
    round(sum(case when date_diff('day', date(A.first_bill_created_dt), date(B.dt)) < 90 then cast(B.payment_amt_local as double) else null end), 2) as total_payments_90_days 
FROM info_gross A
INNER JOIN "db-stage-prod-lf"."payments_cwp" B
     ON cast(A.accountno as varchar) = cast(B.account_id as varchar)
WHERE  
    date(B.dt) between date(A.first_bill_created_dt) - interval '45' day and date(A.first_bill_created_dt) + interval '6' month
GROUP BY 1,2,3
),


early_payments as (
SELECT
    distinct A.serviceno as early_payment_flag
FROM info_gross A
INNER JOIN "db-stage-prod-lf"."payments_cwp" B
     ON cast(A.accountno as varchar) = cast(B.account_id as varchar)
WHERE  
    -- date_trunc(date(B.dt)) between date(A.first_bill_created_dt) - interval '45' day and date(A.first_bill_created_dt) + interval '1' month
    date_trunc('month', date(B.dt)) = (SELECT input_month FROM parameters)
),

gross_pagos as (
SELECT
    *
FROM info_gross A
LEFT JOIN info_pagos B
    ON cast(A.accountno as varchar) = cast(B.accountno_pagos as varchar)
LEFT JOIN early_payments C
    ON cast(A.serviceno as varchar) = cast(C.early_payment_flag as varchar)
),

--------------------------------------------------------------------------------
------------------------------------- NPN ----------------------------------- 
--------------------------------------------------------------------------------

DRC_table AS (
SELECT 
    CAST(act_acct_cd AS VARCHAR) AS accountno_drc,
    ARRAY_AGG(DATE_PARSE(drc_period_final,'%Y-%m') ORDER BY DATE_PARSE(drc_period_final,'%Y-%m'))[1] AS first_drc_date
FROM "lla_cco_int_ext_dev"."drc_movil_new"
GROUP BY act_acct_cd
),

gross_pagos_npn as (
SELECT
    *,
    CASE 
        WHEN pmnt_sell_month_ammnt = NULL THEN 0
        ELSE pmnt_sell_month_ammnt
    END AS Payed_Entry_Fee_ammnt,
    CASE 
        WHEN pmnt_sell_month_ammnt > 20 THEN 'Payed_over_20'
        ELSE 'No_payed_over_20' 
    END AS Payed_over_20_in_sell_month,
    CASE 
        WHEN DATE_DIFF('day',sell_date,first_dt_user) < 5 THEN 'Cliente Existente'
        ELSE 'Cliente Nuevo'
    END AS Cliente_Existente,
    CASE   
        WHEN date_diff('day', date(sell_date), (SELECT current_month FROM parameters)) > 30 and total_payments_30_days IS NULL THEN A.accountno
        WHEN date_diff('day', date(sell_date), (SELECT current_month FROM parameters)) > 30 and total_payments_30_days < total_mrc_d THEN A.accountno 
        ELSE NULL 
    END AS npn_30_flag,
    CASE   
        WHEN date_diff('day', date(sell_date), (SELECT current_month FROM parameters)) > 60 and total_payments_60_days IS NULL THEN A.accountno
        WHEN date_diff('day', date(sell_date), (SELECT current_month FROM parameters)) > 60 and total_payments_60_days < total_mrc_d THEN A.accountno 
        ELSE NULL 
    END AS npn_60_flag,
    CASE   
        WHEN date_diff('day', date(sell_date), (SELECT current_month FROM parameters)) > 90 and total_payments_90_days IS NULL THEN A.accountno
        WHEN date_diff('day', date(sell_date), (SELECT current_month FROM parameters)) > 90 and total_payments_90_days < total_mrc_d THEN A.accountno 
        ELSE NULL 
    END AS npn_90_flag,
    CASE   
        WHEN date_diff('day', date(sell_date), (SELECT current_month FROM parameters)) > 90 and total_payments_in_3_months IS NULL THEN A.accountno
        WHEN date_diff('day', date(sell_date), (SELECT current_month FROM parameters)) > 90 and total_payments_in_3_months < total_mrc_d THEN A.accountno 
        ELSE NULL 
    END AS npn_flag,
    CASE   
        WHEN first_drc_date IS NULL OR B.accountno_drc IS NULL THEN 'No DRC'
        ELSE 'DRC' 
    END AS drc_flag
FROM gross_pagos A
LEFT JOIN drc_table  B
    ON A.accountno = B.accountno_drc
),

--------------------------------------------------------------------------------
---------------------------------- SURVIVING --------------------------------
--------------------------------------------------------------------------------

forward_months as (
SELECT
    distinct A.serviceno,
    A.accountno,
    date_trunc('month', date(A.dt)) as month_survival, 
    C.sell_date, 
    C.province, 
    C.district, 
    C.procedencia, 
    C.sell_channel, 
    C.agent_acc_code, 
    C.npn_30_flag,
    C.npn_60_flag,
    C.npn_90_flag, 
    C.npn_flag, 
    C.early_payment_flag, 
    C.Payed_Entry_Fee_ammnt, 
    C.Payed_over_20_in_sell_month
FROM "db-analytics-prod"."tbl_postpaid_cwp" A
RIGHT JOIN gross_pagos_npn C
    ON cast(A.serviceno as varchar) = cast(C.serviceno as varchar)
    -- ON cast(A.accountno as varchar) = cast(C.accountno as varchar)
WHERE
    A.account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
    and A.category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
    and date(dt) between (select input_month from parameters) and (select input_month from parameters) + interval '13' month
),

survival as (
SELECT

distinct serviceno, 

max(case when (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '0' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '1' month)  and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day) then 1 else null end) as surv_M0,

max(case when (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '1' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) then 1 else null end) as surv_M1,

max(case when (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '2' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) then 1 else null end) as surv_M2,

max(case when (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '3' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) then 1 else null end) as surv_M3,

max(case when (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '4' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) then 1 else null end) as surv_M4,

max(case when (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '5' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) then 1 else null end) as surv_M5,

max(case when (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '6' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) then 1 else null end) as surv_M6,

max(case when (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '7' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) then 1 else null end) as surv_M7,

max(case when (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '8' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) then 1 else null end) as surv_M8,

max(case when (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '9' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) then 1 else null end) as surv_M9,

max(case when (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '10' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) then 1 else null end) as surv_M10,

max(case when (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '11' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) then 1 else null end) as surv_M11,

max(case when (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '12' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) then 1 else null end) as surv_M12

from forward_months 
group by 1
), 

churn as (
SELECT
    distinct B.serviceno, 
    
    case when (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) and surv_m0 is null then 1 else null end as churn_m0,
    
    case when (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) and surv_m1 is null then 1 else null end as churn_m1,
    
    case when (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) and surv_m2 is null then 1 else null end as churn_m2,
    
    case when (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) and surv_m3 is null then 1 else null end as churn_m3,
    
    case when (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) and surv_m4 is null then 1 else null end as churn_m4,
    
    case when (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) and surv_m5 is null then 1 else null end as churn_m5,
    
    case when (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) and surv_m6 is null then 1 else null end as churn_m6,
    
    case when (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) and surv_m7 is null then 1 else null end as churn_m7,
    
    case when (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) and surv_m8 is null then 1 else null end as churn_m8,
    
    case when (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) and surv_m9 is null then 1 else null end as churn_m9,
    
    case when (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) and surv_m10 is null then 1 else null end as churn_m10,
    
    case when (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) and surv_m11 is null then 1 else null end as churn_m11,
    
    case when (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) and surv_m12 is null then 1 else null end as churn_m12
    
FROM forward_months A
LEFT JOIN survival B
    ON A.serviceno = B.serviceno
-- GROUP BY 1
), 

invol_churn as (
SELECT
    distinct A.serviceno, 
    
    case when (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) and surv_m0 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '0' month - interval '1' day) then 1 else null end as invol_m0, 
    
    case when (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) and surv_m1 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '2' month - interval '1' day)) then 1 else null end as invol_m1, 
    
    case when (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) and surv_m2 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '3' month - interval '1' day)) then 1 else null end as invol_m2, 
    
    case when (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) and surv_m3 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '4' month - interval '1' day)) then 1 else null end as invol_m3, 
    
    case when (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) and surv_m4 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '5' month - interval '1' day)) then 1 else null end as invol_m4, 
    
    case when (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) and surv_m5 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '6' month - interval '1' day)) then 1 else null end as invol_m5, 
    
    case when (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) and surv_m6 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '7' month - interval '1' day)) then 1 else null end as invol_m6, 
    
    case when (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) and surv_m7 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '8' month - interval '1' day)) then 1 else null end as invol_m7, 
    
    case when (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) and surv_m8 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '9' month - interval '1' day)) then 1 else null end as invol_m8, 
    
    case when (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) and surv_m9 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '10' month - interval '1' day)) then 1 else null end as invol_m9, 
    
    case when (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) and surv_m10 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '11' month - interval '1' day)) then 1 else null end as invol_m10, 
    
    case when (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) and surv_m11 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '12' month - interval '1' day)) then 1 else null end as invol_m11, 
    
    case when (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) and surv_m12 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '13' month - interval '1' day)) then 1 else null end as invol_m12

-- FROM forward_months A
-- LEFT JOIN survival B
    -- ON A.serviceno = B.serviceno
FROM survival A
ORDER BY A.serviceno
),

vol_churn as (
SELECT 
    distinct A.serviceno,
    
    case when (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) and surv_m0 is null and invol_m0 is null then 1 else null end as vol_m0, 
    
    case when (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) and surv_m1 is null and invol_m0 is null and invol_m1 is null then 1 else null end as vol_m1, 
    
    case when (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) and surv_m2 is null and invol_m1 is null and invol_m2 is null then 1 else null end as vol_m2, 
    
    case when (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) and surv_m3 is null and invol_m2 is null and invol_m3 is null then 1 else null end as vol_m3, 
    
    case when (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) and surv_m4 is null and invol_m3 is null and invol_m4 is null then 1 else null end as vol_m4, 
    
    case when (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) and surv_m5 is null and invol_m4 is null and invol_m5 is null then 1 else null end as vol_m5, 
    
    case when (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) and surv_m6 is null and invol_m5 is null and invol_m6 is null then 1 else null end as vol_m6, 
    
    case when (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) and surv_m7 is null and invol_m6 is null and invol_m7 is null then 1 else null end as vol_m7, 
    
    case when (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) and surv_m8 is null and invol_m7 is null and invol_m8 is null then 1 else null end as vol_m8, 
    
    case when (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) and surv_m9 is null and invol_m8 is null and invol_m9 is null then 1 else null end as vol_m9, 
    
    case when (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) and surv_m10 is null and invol_m9 is null and invol_m10 is null then 1 else null end as vol_m10,
    
    case when (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) and surv_m11 is null and invol_m10 is null and invol_m11 is null then 1 else null end as vol_m11,
    
    case when (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) and surv_m12 is null and invol_m11 is null and invol_m12 is null then 1 else null end as vol_m12
    
-- FROM forward_months A
-- LEFT JOIN survival B
--     ON A.serviceno = B.serviceno
FROM survival A
LEFT JOIN invol_churn C
    ON A.serviceno = C.serviceno
ORDER BY A.serviceno
)

, final_result as (
SELECT
    distinct A.serviceno, 
    A.accountno, 
    date_trunc('month', date(A.sell_date)) as sell_month,
    A.sell_date, 
    A.province, 
    A.district, 
    A.procedencia, 
    A.sell_channel, 
    A.agent_acc_code, 
    A.npn_30_flag,
    A.npn_60_flag,
    A.npn_90_flag, 
    A.npn_flag, 
    A.early_payment_flag, 
    A.Payed_Entry_Fee_ammnt, 
    A.Payed_over_20_in_sell_month,
    surv_m0, surv_m1, surv_m2, surv_m3, surv_m4, surv_m5, surv_m6, surv_m7, surv_m8, surv_m9, surv_m10, surv_m11, surv_m12, 
    churn_m0, churn_m1, churn_m2, churn_m3, churn_m4, churn_m5, churn_m6, churn_m7, churn_m8, churn_m9, churn_m10, churn_m11, churn_m12, 
    invol_m0, invol_m1, invol_m2, invol_m3, invol_m4, invol_m5, invol_m6, invol_m7, invol_m8, invol_m9, invol_m10, invol_m11, invol_m12, 
    vol_m0, vol_m1, vol_m2, vol_m3, vol_m4, vol_m5, vol_m6, vol_m7, vol_m8, vol_m9, vol_m10, vol_m11, vol_m12

FROM forward_months A
LEFT JOIN survival B
    ON A.serviceno = B.serviceno
LEFT JOIN churn C
    ON A.serviceno = C.serviceno
-- LEFT JOIN churn_type D
    -- ON A.serviceno = D.serviceno
LEFT JOIN vol_churn D
    ON A.serviceno = D.serviceno
LEFT JOIN invol_churn E
    ON A.serviceno = E.serviceno
)

SELECT 
    *
FROM final_result


-- LIMIT 10
