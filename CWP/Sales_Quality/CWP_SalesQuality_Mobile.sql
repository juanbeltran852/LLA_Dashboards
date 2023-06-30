--------------------------------------------------------------------------------
-------------------------- CWP - SALES QUALITY MOBILE --------------------------
--------------------------------------------------------------------------------
--- 23/06/2023

WITH

parameters as (
SELECT 
    date('2022-01-01') as input_month, 
    date_trunc('month', date('2023-05-01')) as current_month
),

--------------------------------------------------------------------------------
---------------------------------- Gross Adds ------------------------------
--------------------------------------------------------------------------------

gross_adds AS ( --Tabla Ventas que sale de gross adds
SELECT 
    TRIM(CAST(gross.account AS VARCHAR)) AS accountno, ------- Arround 1k of records do have their 'account' column in null
    gross.service AS serviceno,
    date(DATE_PARSE(TRIM(gross.date),'%m/%d/%Y')) AS sell_date, --Format example 05/27/2023
    -- date_parse(trim(gross.date), '%Y/%m/%d') as date_test,
    gross.channel_resumen AS sell_channel,
    gross.procedencia AS procedencia,
    gross.agent_acc_code AS agent_acc_code,
    gross.plan_name AS plan_name
FROM "db-stage-prod"."gross_ads_movil_b2c_newversion" gross
WHERE
    date_trunc('month', DATE_PARSE(TRIM(gross.date),'%m/%d/%Y')) = (SELECT input_month FROM parameters)
-- LIMIT 1000
),

--- ### ### Candidates sales (Early customers?)
--- Users that were not in the DNA during the previous 6 months

info_early_clients as ( ----- Early clients???
SELECT
    distinct trim(cast(accountno as varchar)) AS accountno, --Account CODE
    trim(cast(serviceno as varchar)) as serviceno,
    case when fi_bill_dt_m0 is null then first_dt_user  else date(fi_bill_dt_m0) end as first_bill_created_dt,
    min(date(dt)) AS first_dt
FROM (
    SELECT     
        DATE(dt) AS dt,
        accountno,
        serviceno,
        fi_bill_dt_m0,
        MIN(DATE(dt)) OVER(PARTITION BY TRIM(CAST(accountno AS VARCHAR))) AS first_dt_user 
    FROM "db-analytics-prod"."tbl_postpaid_cwp"
    WHERE
        account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
        and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
        and date_trunc('month', date(dt)) between (SELECT input_month FROM parameters) - interval '6' month and (SELECT input_month FROM Parameters) --- Use just dt and consider also 1st day of next month?
    GROUP BY 1, 2, 3, 4
    )
WHERE date_trunc('month', first_dt_user) = (SELECT input_month FROM parameters)
GROUP BY accountno, 2, 3
),

--------------------------------------------------------------------------------
---------------------------------- Postpaid DNA -------------------------------
--------------------------------------------------------------------------------

useful_dna as (
SELECT
    date(dt) as dt,
    -- billableaccountno as accountno,
    accountno as accountno_dna,
    serviceno as serviceno_dna,
    first_value(date(dt)) over (partition by trim(cast(billableaccountno as varchar)) order by dt asc) as first_dt_user, --- Can be used as installation_date?
    date(fi_bill_dt_m0) as fi_bill_dt_m0,
    date(fi_bill_due_dt_m0) as fi_bill_due_dt_m0,
    cast(total_mrc_d as double) as total_mrc_d,
    cast(tot_inv_mo as double) AS tot_inv_mo,
    account_status as account_status,
    category as category, 
    province as province, 
    district as district
FROM "db-analytics-prod"."tbl_postpaid_cwp"
WHERE
    account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
    and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters) -------- Why should I consider a time range between the input month and 4 months ahead?
),

info_gross as (
SELECT
    distinct A.accountno, 
    A.serviceno, 
    A.sell_date, 
    A.sell_channel, 
    A.procedencia, 
    A.agent_acc_code, 
    A.plan_name, 
    B.first_dt_user, 
    B.total_mrc_d, 
    -- B.account_status, 
    B.category, 
    B.province, 
    B.district
FROM gross_adds A
INNER JOIN useful_dna B
    -- ON A.accountno = B.accountno_dna
    ON cast(A.serviceno as varchar) = cast(B.serviceno_dna as varchar)
-- WHERE date(B.dt) = (SELECT input_month FROM parameters)
),

/* This part is about first bill generated. As we rely on the DRC file and the Polaris info is being tested, then I won't be using that column for the moment.

,bills_of_interest AS (
SELECT act_acct_cd,
    -- Usamos la fecha de la primera factura generada y no del oldes_unpaid_bill para no ser susceptibles a errores en el fi_outst_age o oldet_unpaid_bill
    DATE(TRY(FILTER(ARRAY_AGG(fi_bill_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1])) AS first_bill_created
FROM sales_base
GROUP BY act_acct_cd 
)
*/

--------------------------------------------------------------------------------
------------------------------------- PAYMENTS -----------------------------------
--------------------------------------------------------------------------------

info_pagos as (
SELECT
    cast(early.accountno as varchar) as accountno_pagos, 
    early.first_bill_created_dt as first_bill_created_dt, 
    --- Array 1
    --- Array 2
    --- Array 3
    --- Array 4
    round(sum(cast(payment_amt_local as double)), 2) as total_payments_in_3_months, 
    round(sum(case when date_diff('day', date(early.first_bill_created_dt), dt) < 30 then cast(pmnts.payment_amt_local as double) else null end), 2) as total_payments_30_days,
    round(sum(case when date_diff('day', date(early.first_bill_created_dt), dt) < 60 then cast(pmnts.payment_amt_local as double) else null end), 2) as total_payments_60_days,
    round(sum(case when date_diff('day', date(early.first_bill_created_dt), dt) < 90 then cast(pmnts.payment_amt_local as double) else null end), 2) as total_payments_90_days
FROM (
    SELECT
        first_bill_created_dt as first_bill_created_dt, 
        first_dt as first_dt, 
        cast(accountno as varchar) as accountno
        -- cast(serviceno as varchar) as serviceno
    FROM info_early_clients
    ) early
INNER JOIN (
    SELECT
        distinct cast(account_id as varchar) as accountno, 
        date(dt) as dt, 
        cast(payment_amt_local as double) as payment_amt_local
    FROM "db-stage-prod-lf"."payments_cwp"
    ) pmnts
ON cast(pmnts.accountno as varchar) = cast(early.accountno as varchar)
WHERE
    date(pmnts.dt) between date(early.first_bill_created_dt) - interval '45' day and date(early.first_bill_created_dt) + interval '3' month
GROUP BY early.accountno, early.first_bill_created_dt
),

-- info_pago_adelantado ???

gross_pagos as (
SELECT
    *
FROM info_gross A
LEFT JOIN info_pagos B
    ON A.accountno = B.accountno_pagos
),

--------------------------------------------------------------------------------
---------------------------- INVOLUNTARY CHURN AND NPN --------------------------
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
    -- CASE 
    --     WHEN pmnt_sell_month_ammnt = NULL THEN 0
    --     ELSE pmnt_sell_month_ammnt
    -- END AS Payed_Entry_Fee_ammnt,
    -- CASE 
    --     WHEN pmnt_sell_month_ammnt > 20 THEN 'Payed_over_20'
    --     ELSE 'No_payed_over_20' 
    -- END AS Payed_over_20_sell_month,
    CASE 
        WHEN DATE_DIFF('day',sell_date,first_dt_user) < 5 THEN 'Cliente Existente'
        ELSE 'Cliente Nuevo'
    END AS Cliente_Existente,
    CASE   
        WHEN total_payments_30_days IS NULL THEN A.accountno
        WHEN total_payments_30_days < total_mrc_d THEN A.accountno 
        ELSE NULL 
    END AS npn_30_flag,
    CASE   
        WHEN total_payments_60_days IS NULL THEN A.accountno
        WHEN total_payments_60_days < total_mrc_d THEN A.accountno 
        ELSE NULL 
    END AS npn_60_flag,
    CASE   
        WHEN total_payments_90_days IS NULL THEN A.accountno
        WHEN total_payments_90_days < total_mrc_d THEN A.accountno 
        ELSE NULL 
    END AS npn_90_flag,
    CASE   
        WHEN total_payments_in_3_months IS NULL THEN A.accountno
        WHEN total_payments_in_3_months < total_mrc_d THEN A.accountno 
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
---------------------------------- MORTALITY RATE --------------------------------
--------------------------------------------------------------------------------

-- forward_months as (
-- Select date_trunc('MONTH', date(dt)) as month_survival,dt, act_acct_cd, fi_outst_age,case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
-- from "db-analytics-prod-lf"."dna_fixed_cwp" 
-- where date(dt) = date_trunc('MONTH',date(dt)) + interval '1' month - interval '1' day and date(dt) between (select input_month from parameters) and (select input_month from parameters) + interval '13' month
-- and act_acct_cd in (select distinct act_acct_cd from (select * from part_one))
-- and act_acct_stat != 'C' and pd_mix_cd != '0P'
-- )

forward_months as (
SELECT
    distinct A.serviceno,
    A.accountno,
    date_trunc('month', date(A.dt)) as month_survival, 
    -- A.dt, 
    -- A.accountno,
    -- A.serviceno,
    -- B.accountno as invol_churn_flag, 
    -- C.first_bill_created_dt, 
    C.sell_date, 
    C.province, 
    C.district, 
    C.procedencia, 
    C.sell_channel, 
    C.agent_acc_code, 
    C.npn_30_flag,
    C.npn_60_flag,
    C.npn_90_flag, 
    C.npn_flag
FROM "db-analytics-prod"."tbl_postpaid_cwp" A
RIGHT JOIN gross_pagos_npn C
    ON cast(A.serviceno as varchar) = cast(C.serviceno as varchar)
    -- ON cast(A.accountno as varchar) = cast(C.accountno as varchar)
WHERE
    A.account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
    and A.category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
    and date(A.dt) = date_trunc('MONTH',date(dt)) + interval '1' month - interval '1' day and date(dt) between (select input_month from parameters) and (select input_month from parameters) + interval '13' month
),

acct_panel_surv as (
select 
month_survival,
date_trunc('month', date(sell_date)) as sales_month, 
accountno,
serviceno,
-- province,
-- district,
-- procedencia, 
-- sell_channel, 
-- agent_acc_code as cod_sales_rep, 
-- npn_30_flag, 
-- npn_60_flag, 
-- npn_90_flag,
npn_flag,
-- case
    -- when cast(accountno as varchar) in 
-- max(first_bill_created_dt) as max_oldest_unpaid_bill_dt,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '0' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '1' month)  and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day) then 1 else null end) as surv_M0,
-- max(case when month_survival = (select input_month from parameters) + interval '0' month then fi_outst_age else null end) as fi_outst_age_M0,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '1' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) then 1 else null end) as surv_M1,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '2' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) then 1 else null end) as surv_M2,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '3' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) then 1 else null end)as surv_M3,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '4' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) then 1 else null end) as surv_M4,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '5' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) then 1 else null end) as surv_M5,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '6' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) then 1 else null end) as surv_M6,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '7' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) then 1 else null end) as surv_M7,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '8' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) then 1 else null end) as surv_M8,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '9' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) then 1 else null end) as surv_M9,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '10' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) then 1 else null end) as surv_M10,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '11' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) then 1 else null end) as surv_M11,
max(case when month_survival = (SELECT input_month FROM parameters) + interval '12' month and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) and cast(accountno as varchar) not in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) then 1 else null end) as surv_M12

-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '0' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '0' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '1' month) then 1
-- else null end) as churn_M0, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '1' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '1' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) then 1
-- else null end) as churn_M1, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '2' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '2' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) then 1
-- else null end) as churn_M2, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '3' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '3' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) then 1
-- else null end) as churn_M3, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '4' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '4' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) then 1
-- else null end) as churn_M4, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '5' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '5' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) then 1
-- else null end) as churn_M5,
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '6' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '6' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) then 1
-- else null end) as churn_M6,
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '7' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '7' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) then 1
-- else null end) as churn_M7, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '8' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '8' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) then 1
-- else null end) as churn_M8, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '9' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '9' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) then 1
-- else null end) as churn_M9, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '10' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '10' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) then 1
-- else null end) as churn_M10, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '11' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '11' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) then 1
-- else null end) as churn_M11, 
-- max(case 
--     when month_survival = (SELECT input_month FROM parameters) + interval '12' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) then 1 
--     when month_survival = (SELECT input_month FROM parameters) + interval '12' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) then 1
-- else null end) as churn_M12 

from forward_months 
group by 1,2,3,4,5--,6,7,8,9,10,11
),

panel_churntype as (
SELECT
    *, 
max(case  
    when surv_m0 <> 1 and (month_survival = (SELECT input_month FROM parameters) + interval '0' month) and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day) then 'Involuntary churn' 
    when surv_m0 <> 1 and (month_survival = (SELECT input_month FROM parameters) + interval '0' month) and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '1' month) then 'Voluntary churner'
    else null end) as churntype_M0, 
max(case  
    when surv_m1 <> 1 and (month_survival = (SELECT input_month FROM parameters) + interval '1' month) and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) then 'Involuntary churn' 
    when surv_m1 <> 1 and (month_survival = (SELECT input_month FROM parameters) + interval '1' month) and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M1, 
max(case  
    when /*surv_m2 <> 1 and*/ (month_survival = (SELECT input_month FROM parameters) + interval '2' month) and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) then 'Involuntary churner' 
    when /*surv_m2 <> 1 and*/ (month_survival = (SELECT input_month FROM parameters) + interval '2' month) and cast(serviceno as varchar) not in (SELECT cast(serviceno as varchar) FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE /*account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and */ date(dt) = (SELECT input_month FROM parameters) + interval '3' month /*- interval '1' day*/) then 'Voluntary churner'
    else null end) as churntype_M2, 
max(case  
    when surv_m3 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '3' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) then 'Involuntary churn' 
    when surv_m3 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '3' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M3, 
max(case  
    when surv_m4 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '4' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) then 'Involuntary churn' 
    when surv_m4 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '4' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M4, 
max(case  
    when surv_m5 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '5' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) then 'Involuntary churn' 
    when surv_m5 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '5' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M5, 
max(case  
    when surv_m6 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '6' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) then 'Involuntary churn' 
    when surv_m6 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '6' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M6, 
max(case  
    when surv_m7 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '7' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) then 'Involuntary churn' 
    when surv_m7 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '7' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M7, 
max(case  
    when surv_m8 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '8' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) then 'Involuntary churn' 
    when surv_m8 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '8' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M8, 
max(case  
    when surv_m9 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '9' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) then 'Involuntary churn' 
    when surv_m9 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '9' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M9, 
max(case  
    when surv_m10 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '10' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) then 'Involuntary churn' 
    when surv_m10 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '10' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M10, 
max(case  
    when surv_m11 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '11' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) then 'Involuntary churn' 
    when surv_m11 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '11' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M11, 
max(case  
    when surv_m12 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '12' month and cast(accountno as varchar) in (SELECT cast(act_acct_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) then 'Involuntary churn' 
    when surv_m12 <> 1 and month_survival = (SELECT input_month FROM parameters) + interval '12' month and cast(serviceno as varchar) not in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) then 'Voluntary churner'
    else null end) as churntype_M12 
FROM acct_panel_surv
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

-- SELECT
-- --     sum(surv_m0) as m0, 
-- --     sum(surv_m1) as m1, 
-- --     sum(surv_m2) as m2, 
-- --     sum(surv_m3) as m3, 
-- --     sum(surv_m4) as m4, 
-- --     sum(surv_m5) as m5, 
-- --     sum(surv_m6) as m6, 
-- --     sum(surv_m7) as m7,
-- --     sum(surv_m8) as m8,
-- --     sum(surv_m9) as m9, 
-- --     sum(surv_m10) as m10, 
-- --     sum(surv_m11) as m11, 
-- --     sum(surv_m12) as m12,

--     sum(churn_M0) as churn_m0, 
--     sum(churn_M1) as churn_m1, 
--     sum(churn_M2) as churn_M2, 
--     sum(churn_M3) as churn_M3, 
--     sum(churn_M4) as churn_M4, 
--     sum(churn_M5) as churn_M5, 
--     sum(churn_M6) as churn_M6, 
--     sum(churn_M7) as churn_M7, 
--     sum(churn_M8) as churn_M8, 
--     sum(churn_M9) as churn_M9, 
--     sum(churn_M10) as churn_M10, 
--     sum(churn_M11) as churn_M11, 
--     sum(churn_M12) as churn_M12
    
-- FROM acct_panel_surv

SELECT
    distinct churntype_m2, count(distinct serviceno)
FROM panel_churntype
GROUP BY 1

-- SELECT
-- --     -- count(distinct accountno)
-- --     -- count(distinct serviceno)
--     count(distinct npn_flag)
-- --     *
-- FROM acct_panel_surv
-- -- LIMIT 10
