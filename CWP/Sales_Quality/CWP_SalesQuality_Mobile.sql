--------------------------------------------------------------------------------
-------------------------- CWP - SALES QUALITY MOBILE --------------------------
--------------------------------------------------------------------------------
--- Commented in 06/07/2023 (d/m/y).

WITH

parameters as (
SELECT 
    date('2023-01-01') as input_month,  --- The month we want to obtain the results for
    date_trunc('month', date('2023-06-01')) as current_month --- The last month of available data
),

--------------------------------------------------------------------------------
---------------------------------- Gross Adds ------------------------------
--------------------------------------------------------------------------------

---
--- The gross adds table is the most important input, it allows us to identify the sales of the month
--- and take the relevant information
---

gross_adds AS (
SELECT 
    TRIM(CAST(gross.account AS VARCHAR)) AS accountno, ------- Account_id
    gross.service AS serviceno, ------ Each service number is a different phone number in Panama. We build the detail for each phone number.
    date(DATE_PARSE(TRIM(gross.date),'%m/%d/%Y')) AS sell_date, -- Target format example: 05/27/2023
    gross.channel_resumen AS sell_channel, 
    gross.procedencia AS procedencia, --- A phone number can come as a completely new number, as a previous prepaid customer or as a number brought from another operator.
    activado_en as activation_channel,
    case when lower(substr(trim(gross.agent_acc_code), 1, position('-' in gross.agent_acc_code) - 1)) like '%none%' then null else substr(upper(trim(gross.agent_acc_code)), 1, position('-' in gross.agent_acc_code) - 1) end AS agent_acc_code, --- The column shows something like 'ABC100 - John Doe' so we separate the code and the name of the sales agent. Also, there are various records like 'NONE - NONE' so they are replaced with null values.
    case when lower(trim(substr(gross.agent_acc_code, position('-' in gross.agent_acc_code) + 2, length(gross.agent_acc_code)))) like '%none%' then null else trim(substr(upper(gross.agent_acc_code), position('-' in gross.agent_acc_code) + 2, length(gross.agent_acc_code))) end as sales_agent_name,
    gross.plan_name AS plan_name, 
    case when (gross.marca is null or lower(gross.marca) like '%no%handset%' or lower(gross.marca) in ('', ' ')) then null else service end as handsets_flag, --- This flags customers when they are given a new cellphone with their new phone number.
    gross.plan_id as plan_code
FROM "db-stage-prod"."gross_ads_movil_b2c_newversion" gross
WHERE
    date_trunc('month', DATE_PARSE(TRIM(gross.date),'%m/%d/%Y')) = (SELECT input_month FROM parameters)
), 

--------------------------------------------------------------------------------
---------------------------------- Postpaid DNA -------------------------------
--------------------------------------------------------------------------------

---
--- From the DNA we extract more relevant information that couldn't be found in the 
--- gross adds table.
---

useful_dna as (
SELECT
    -- billableaccountno as accountno,
    distinct serviceno as serviceno_dna,
    accountno as accountno_dna,
    upper(trim(customerdesc)) as client_name,
    first_value(date(dt)) over (partition by trim(cast(billableaccountno as varchar)) order by dt asc) as first_dt_user, --- First appearance in the DNA of the sale.
    date(fi_bill_dt_m0) as fi_bill_dt_m0, --- Issuance date of the bill of this month (first bill)
    date(fi_bill_due_dt_m0) as fi_bill_due_dt_m0, --- Limit date of the bill of this month (first bill)
    cast(total_mrc_d as double) as total_mrc_d,
    cast(tot_inv_mo as double) AS tot_inv_mo,
    account_status as account_status,
    category as category, 
    first_value(province) over (partition by serviceno order by dt desc) as province,
    first_value(district) over (partition by serviceno order by dt desc) as district,
    first_value(date(concat(substr(startdate_serviceno,1,4),'-',substr(startdate_serviceno,6,2),'-',substr(startdate_serviceno, 9,2)))) over (partition by serviceno order by dt asc) as activation_date, --- Some adjustments were required due to the base format of the column
    -- first_value(salesmanname) over (partition by serviceno order by dt asc) as sales_agent_name, --- An alternative to obtain the sales agent. It may not match with the name displayed in the gross adds table.
    date(dt) as dt
FROM "db-analytics-prod"."tbl_postpaid_cwp"
WHERE
    account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
    and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
    -- and total_mrc_d not in ('CORP 900') --- October 2022 data issue
    -- and tot_inv_mo not in ('CORP 900') --- October 2022 data issue
),

---
--- Joining the dna info and the gross adds table info together.
---

info_gross as (
SELECT
    distinct A.serviceno, 
    A.accountno, 
    A.sell_date, 
    first_value(A.sell_channel) over (partition by A.serviceno order by A.sell_channel) as sell_channel,
    first_value(A.procedencia) over (partition by A.serviceno order by A.procedencia) as procedencia,
    first_value(A.agent_acc_code) over (partition by A.serviceno order by A.agent_acc_code) as agent_acc_code,
    A.sales_agent_name,
    A.activation_channel,
    B.client_name,
    B.first_dt_user, 
    B.total_mrc_d,
    B.category, 
    B.province, 
    B.district, 
    B.activation_date,
    A.handsets_flag,
    A.plan_code, 
    A.plan_name,
    case when B.fi_bill_dt_m0 is null then B.first_dt_user else date(fi_bill_dt_m0) end as first_bill_created_dt
FROM gross_adds A
INNER JOIN useful_dna B --- We keep just the commond records in both tables
    ON cast(A.serviceno as varchar) = cast(B.serviceno_dna as varchar)
),

--------------------------------------------------------------------------------
------------------------------------- PAYMENTS -----------------------------------
--------------------------------------------------------------------------------

---
--- We are interested in the payments table because we want to identify NPN.
--- Additionally, we want to flag customers with early payments.
---

info_pagos as (
SELECT
    distinct cast(A.serviceno as varchar) as serviceno_pagos, 
    cast(A.accountno as varchar) as accountno_pagos, 
    A.first_bill_created_dt, 
    round(SUM(TRY_CAST(payment_amt_local AS DOUBLE)), 2) AS pmnt_sell_month_ammnt,
    round(sum(case when date(B.dt) <= activation_date then TRY_CAST(payment_amt_local as double) else null end),2) as pmnt_activation_dt, --- Current definition for early payments: Users that, at their activation date, have already paid at least USD$20.
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

---
--- Joining the info together
---

gross_pagos as (
SELECT
    *
FROM info_gross A
LEFT JOIN info_pagos B
    ON cast(A.accountno as varchar) = cast(B.accountno_pagos as varchar)
-- LEFT JOIN early_payments C
    -- ON cast(A.serviceno as varchar) = cast(C.early_payment_flag as varchar)
),

--------------------------------------------------------------------------------
------------------------------------- NPN ----------------------------------- 
--------------------------------------------------------------------------------

---
--- In the derecognition table we can identify involuntary churners
---

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
        WHEN cast(pmnt_activation_dt as double) >= 20 THEN A.serviceno --- Here we flag the early payments cosidering if the user paid at least USD$20 before their activation date.
        ELSE null 
    END AS early_payment_flag,
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
    END AS drc_flag --- Flag used in the queries send by the OpCo. We do not use this column anymore.
FROM gross_pagos A
LEFT JOIN drc_table  B
    ON A.accountno = B.accountno_drc
),

--------------------------------------------------------------------------------
---------------------------------- SURVIVAL -----------------------------------
--------------------------------------------------------------------------------

--- Now, we are going to track the perfomance of the different phone numbers identified as sales
--- in the input month. In particular, we are going to check if the users churned or not in the next 12 months.
--- If we do not have information for the next 12 months, the code will give you results according to the current_month
--- parameter.

forward_months as (
SELECT
    distinct A.serviceno,
    A.accountno,
    date_trunc('month', date(A.dt)) as month_survival, --- We'll use this column to track each of the relevant months.
    C.sell_date, 
    C.activation_date,
    C.client_name,
    C.province, 
    C.district, 
    C.procedencia, 
    C.sell_channel, 
    C.agent_acc_code, 
    C.sales_agent_name,
    C.activation_channel,
    C.handsets_flag,
    C.plan_code,
    C.plan_name,
    C.total_mrc_d,
    C.npn_30_flag,
    C.npn_60_flag,
    C.npn_90_flag, 
    C.npn_flag, 
    C.early_payment_flag, 
    C.Payed_Entry_Fee_ammnt
FROM "db-analytics-prod"."tbl_postpaid_cwp" A
RIGHT JOIN gross_pagos_npn C
    ON cast(A.serviceno as varchar) = cast(C.serviceno as varchar)
    -- ON cast(A.accountno as varchar) = cast(C.accountno as varchar)
WHERE
    A.account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
    and A.category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
    and date(dt) between (select input_month from parameters) and (select input_month from parameters) + interval '13' month --- We take 13  months ahead in case we need information of the next month to check anything in particular.
),

---
--- In the survival subquery we assign, for each month, a 1 in case these two conditions are met (both of them are required): 
--- 1. The service number can still be found in the last day of the month Postpaid DNA.
--- 2. The account number cannot be identified as an involuntary churner of the month. We can check either the DRC file or the accounts with overdue > 90 in Postpaid Campaigns table. At the moment, we are focused in the second source.
--- If these conditions couldn't be met, we can conclude that users didn't survive in the respective month.
---

survival as (
SELECT

distinct serviceno, 
accountno,

max(
    case when 
        (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) --- Here we are making sure that we are limited by the desired date.
        and month_survival = (SELECT input_month FROM parameters) + interval '0' month 
        and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '1' month)  
        -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day) then 1 else null end) as surv_M0, --- DRC check for discarting involuntary churner
        and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day and "dias_de_atraso" > 90) --- Polaris Campaigns check for discarting involuntary,
        then 1 else null end) as surv_m0,
max(case when 
    (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '1' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) then 1 else null end) as surv_M1,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '2' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m1,

max(case when 
    (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '2' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) then 1 else null end) as surv_M2,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '3' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m2,

max(case when 
    (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters)
    and month_survival = (SELECT input_month FROM parameters) + interval '3' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) then 1 else null end) as surv_M3,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '4' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m3,

max(case when 
    (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '4' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) then 1 else null end) as surv_M4,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '5' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m4,

max(case when 
    (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '5' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) then 1 else null end) as surv_M5,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '6' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m5,

max(case when 
    (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '6' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) then 1 else null end) as surv_M6,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '7' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m6,

max(case when 
    (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '7' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) then 1 else null end) as surv_M7,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '8' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m7,

max(case when 
    (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '8' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) then 1 else null end) as surv_M8,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '9' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m8,

max(case when 
    (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '9' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) then 1 else null end) as surv_M9,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '10' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m9,

max(case when 
    (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '10' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) then 1 else null end) as surv_M10,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '11' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m10,

max(case when 
    (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '11' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) then 1 else null end) as surv_M11,
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '12' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m11,

max(case when 
    (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) 
    and month_survival = (SELECT input_month FROM parameters) + interval '12' month 
    and cast(serviceno as varchar) in (SELECT serviceno FROM "db-analytics-prod"."tbl_postpaid_cwp" WHERE account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS') and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees') and date(dt) = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) 
    -- and cast(serviceno as varchar) not in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) then 1 else null end) as surv_M12
    and cast(accountno as varchar) not in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '13' month - interval '1' day and "dias_de_atraso" > 90)
    then 1 else null end) as surv_m12

from forward_months 
group by 1, 2
), 

--------------------------------------------------------------------------------
---------------------------------- CHURN --------------------------------
--------------------------------------------------------------------------------

---
--- Now, we are going to build the opposite to the survival waterfall. This is assigning 
--- a 1 in each month that the account couldn't be clasified as an active one.
---

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

---
--- If the user was active in a previous month but did churn in the one we are checking, we identify the churntype. Thus, 
--- the churntype will not appear each month since the account is not active but only in the first month in which we put the churn flag.
--- We first check involuntary churn and then voluntary churn (as a complement).
--- Involuntary: We check if the account can be found in the DRC table or do have an overdue > 90 in the Postpaid Campaigns table cannot be found in the last day of the month in the Postpaid DNA.
--- Voluntary: We check if the account cannot be found in the last day of the month in the Postpaid DNA.
--- Thus, if a churned account cannot be associated to involuntary churn, it will be clasified as a voluntary churner.
---

churn_type as (
SELECT
    distinct A.serviceno,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '0' month and surv_m0 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '1' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) 
            and month_survival = (SELECT input_month FROM parameters) + interval '0' month 
            and churn_m0 = 1 /*surv_m0 is null*/
            and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '1' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '0' month and churn_m1 = 1 then 'Voluntario'
    else null end as churntype_m0,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '1' month and surv_m0 = 1 and surv_m1 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '2' month - interval '1' day) then 'Involuntario'
        when 
            (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) 
            and month_survival = (SELECT input_month FROM parameters) + interval '1' month 
            and churn_m0 is null and churn_m1 = 1 /*surv_m0 = 1 and surv_m1 is null*/  --- Here we are checking if this is the first churn month of the account.
            and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '2' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '1' month and surv_m0 = 1 and surv_m1 is null then 'Voluntario'
    else null end as churntype_m1,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '2' month and surv_m1 = 1 and surv_m2 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '3' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '2' month and churn_m1 is null and churn_m2 = 1 /*surv_m1 = 1 and surv_m2 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '3' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '2' month and surv_m1 = 1 and surv_m2 is null then 'Voluntario'
    else null end as churntype_m2,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '3' month and surv_m2 = 1 and surv_m3 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '4' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '3' month and churn_m2 is null and churn_m3 = 1 /*surv_m2 = 1 and surv_m3 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '4' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '3' month and surv_m2 = 1 and surv_m3 is null then 'Voluntario'
    else null end as churntype_m3,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '4' month and surv_m3 = 1 and surv_m4 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '5' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '4' month and churn_m3 is null and churn_m4 = 1 /*surv_m3 = 1 and surv_m4 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '5' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '4' month and surv_m3 = 1 and surv_m4 is null then 'Voluntario'
    else null end as churntype_m4,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '5' month and surv_m4 = 1 and surv_m5 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '6' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '5' month and churn_m4 is null and churn_m5 = 1 /*surv_m4 = 1 and surv_m5 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '6' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario'  
        when (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '5' month and surv_m4 = 1 and surv_m5 is null then 'Voluntario'
    else null end as churntype_m5,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '6' month and surv_m5 = 1 and surv_m6 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '7' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '6' month and churn_m5 is null and churn_m6 = 1 /*surv_m5 = 1 and surv_m6 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '7' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario'  
        when (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '6' month and surv_m5 = 1 and surv_m6 is null then 'Voluntario'
    else null end as churntype_m6,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '7' month and surv_m6 = 1 and surv_m7 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '8' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '7' month and churn_m6 is null and churn_m7 = 1 /*surv_m6 = 1 and surv_m7 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '8' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario'  
        when (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '7' month and surv_m6 = 1 and surv_m7 is null then 'Voluntario'
    else null end as churntype_m7,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '8' month and surv_m7 = 1 and surv_m8 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '9' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '8' month and churn_m7 is null and churn_m8 = 1 /*surv_m7 = 1 and surv_m8 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '9' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario' 
        when (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '8' month and surv_m7 = 1 and surv_m8 is null then 'Voluntario'
    else null end as churntype_m8,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '9' month and surv_m8 = 1 and surv_m9 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '10' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '9' month and churn_m8 is null and churn_m9 = 1 /*surv_m8 = 1 and surv_m9 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '10' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario' 
        when (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '9' month and surv_m8 = 1 and surv_m9 is null then 'Voluntario'
    else null end as churntype_m9,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '10' month and surv_m9 = 1 and surv_m10 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '11' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '10' month and churn_m9 is null and churn_m10 = 1 /*surv_m9 = 1 and surv_m10 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '11' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario' 
        when (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '10' month and surv_m9 = 1 and surv_m10 is null then 'Voluntario'
    else null end as churntype_m10,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '11' month and surv_m10 = 1 and surv_m11 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '12' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '11' month and churn_m10 is null and churn_m11 = 1/*surv_m10 = 1 and surv_m11 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '12' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario' 
        when (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '11' month and surv_m10 = 1 and surv_m11 is null then 'Voluntario'
    else null end as churntype_m11,
    case 
        -- when month_survival = (SELECT input_month FROM parameters) + interval '12' month and surv_m11 = 1 and surv_m12 is null and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '13' month - interval '1' day) then 'Involuntario'
        when (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '12' month and churn_m11 is null and churn_m12 = 1 /*surv_m11 = 1 and surv_m12 is null*/ and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '13' month - interval '1' day and "dias_de_atraso" > 90) then 'Involuntario' 
        when (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) and month_survival = (SELECT input_month FROM parameters) + interval '12' month and surv_m11 = 1 and surv_m12 is null then 'Voluntario'
    else null end as churntype_m12
    
FROM forward_months A
LEFT JOIN survival B
    ON A.serviceno = B.serviceno
LEFT JOIN churn C
    ON A.serviceno = C.serviceno
ORDER BY A.serviceno
),

--------------------------------------------------------------------------------
---------------------------- INVOLUNTARY AND VOLUNTARY CHURN ---------------------
--------------------------------------------------------------------------------

---
--- In this section 2 different waterfalls were built, one for involuntary churn and
--- the other for voluntary churn. Although they were not useful for the DNA, they might be
--- useful for other purposes.
---

-- invol_churn as (
-- SELECT
--     distinct A.serviceno, 
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) 
--         and surv_m0 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') = (SELECT input_month FROM parameters) + interval '0' month - interval '1' day) then 1 else null end as invol_m0, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '1' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m0,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) 
--         and surv_m1 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '2' month - interval '1' day)) then 1 else null end as invol_m1, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '2' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m1,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) 
--         and surv_m2 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '3' month - interval '1' day)) then 1 else null end as invol_m2, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '3' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m2,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) 
--         and surv_m3 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '4' month - interval '1' day)) then 1 else null end as invol_m3, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '4' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m3,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) 
--         and surv_m4 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '5' month - interval '1' day)) then 1 else null end as invol_m4, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '5' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m4,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) 
--         and surv_m5 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '6' month - interval '1' day)) then 1 else null end as invol_m5, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '6' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m5,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) 
--         and surv_m6 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '7' month - interval '1' day)) then 1 else null end as invol_m6, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '7' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m6,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) 
--         and surv_m7 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '8' month - interval '1' day)) then 1 else null end as invol_m7, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '8' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m7,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) 
--         and surv_m8 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '9' month - interval '1' day)) then 1 else null end as invol_m8, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '9' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m8,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) 
--         and surv_m9 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '10' month - interval '1' day)) then 1 else null end as invol_m9, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '10' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m9,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) 
--         and surv_m10 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '11' month - interval '1' day)) then 1 else null end as invol_m10, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '11' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m10,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) 
--         and surv_m11 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '12' month - interval '1' day)) then 1 else null end as invol_m11, 
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '12' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m11,
    
--     case when 
--         (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) 
--         and surv_m12 is null 
--         -- and cast(A.serviceno as varchar) in (SELECT cast(act_service_cd as varchar) FROM "lla_cco_int_ext_dev"."drc_movil_new" WHERE date_parse(fecha_drc,'%m/%d/%Y%') between ((SELECT input_month FROM parameters) + interval '0' month - interval '1' day) and ((SELECT input_month FROM parameters) + interval '13' month - interval '1' day)) then 1 else null end as invol_m12
--         and cast(A.accountno as varchar) in (SELECT cast(billableaccountno as varchar) FROM "db-stage-dev"."polaris_campaigns" WHERE date(dt) = (SELECT input_month FROM parameters)  + interval '13' month - interval '1' day and "dias_de_atraso" > 90)
--         then 1 else null end as invol_m12

-- -- FROM forward_months A
-- -- LEFT JOIN survival B
--     -- ON A.serviceno = B.serviceno
-- FROM survival A
-- ORDER BY A.serviceno
-- ),

-- vol_churn as (
-- SELECT 
--     distinct A.serviceno,
    
--     case when (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) and surv_m0 is null and invol_m0 is null then 1 else null end as vol_m0, 
    
--     case when (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) and surv_m1 is null and invol_m0 is null and invol_m1 is null then 1 else null end as vol_m1, 
    
--     case when (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) and surv_m2 is null and invol_m1 is null and invol_m2 is null then 1 else null end as vol_m2, 
    
--     case when (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) and surv_m3 is null and invol_m2 is null and invol_m3 is null then 1 else null end as vol_m3, 
    
--     case when (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) and surv_m4 is null and invol_m3 is null and invol_m4 is null then 1 else null end as vol_m4, 
    
--     case when (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) and surv_m5 is null and invol_m4 is null and invol_m5 is null then 1 else null end as vol_m5, 
    
--     case when (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) and surv_m6 is null and invol_m5 is null and invol_m6 is null then 1 else null end as vol_m6, 
    
--     case when (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) and surv_m7 is null and invol_m6 is null and invol_m7 is null then 1 else null end as vol_m7, 
    
--     case when (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) and surv_m8 is null and invol_m7 is null and invol_m8 is null then 1 else null end as vol_m8, 
    
--     case when (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) and surv_m9 is null and invol_m8 is null and invol_m9 is null then 1 else null end as vol_m9, 
    
--     case when (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) and surv_m10 is null and invol_m9 is null and invol_m10 is null then 1 else null end as vol_m10,
    
--     case when (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) and surv_m11 is null and invol_m10 is null and invol_m11 is null then 1 else null end as vol_m11,
    
--     case when (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) and surv_m12 is null and invol_m11 is null and invol_m12 is null then 1 else null end as vol_m12
    
-- -- FROM forward_months A
-- -- LEFT JOIN survival B
-- --     ON A.serviceno = B.serviceno
-- FROM survival A
-- LEFT JOIN invol_churn C
--     ON A.serviceno = C.serviceno
-- ORDER BY A.serviceno
-- ),

--------------------------------------------------------------------------------
---------------------------------- ARPU (ARPC): MRC ---------------------------
--------------------------------------------------------------------------------

---
--- Now, we display the MRC faced by each user in the different months of analysis.
--- If an accounts is not active anymore, a null value will be displayed.
---

forward_months_mrc as (
SELECT
    distinct A.serviceno,
    A.accountno,
    date_trunc('month', date(A.dt)) as month_forward, 
    max(C.total_mrc_d) as max_total_mrc
FROM "db-analytics-prod"."tbl_postpaid_cwp" A
RIGHT JOIN gross_pagos_npn C
    ON cast(A.serviceno as varchar) = cast(C.serviceno as varchar)
    -- ON cast(A.accountno as varchar) = cast(C.accountno as varchar)
WHERE
    A.account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
    and A.category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
    and date(dt) between (select input_month from parameters) and (select input_month from parameters) + interval '13' month
GROUP BY 1, 2, 3
),

mrc_evol as (
SELECT
    distinct A.serviceno, 
    
    max(case when surv_m0 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '0' month and (SELECT input_month FROM parameters) + interval '0' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m0, 
    
    max(case when surv_m1 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '1' month and (SELECT input_month FROM parameters) + interval '1' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m1, 
    
    max(case when surv_m2 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '2' month and (SELECT input_month FROM parameters) + interval '2' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m2, 
    
    max(case when surv_m3 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '3' month and (SELECT input_month FROM parameters) + interval '3' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m3, 
    
    max(case when surv_m4 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '4' month and (SELECT input_month FROM parameters) + interval '4' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m4, 
    
    max(case when surv_m5 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '5' month and (SELECT input_month FROM parameters) + interval '5' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m5, 
    
    max(case when surv_m6 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '6' month and (SELECT input_month FROM parameters) + interval '6' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m6, 
    
    max(case when surv_m7 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '7' month and (SELECT input_month FROM parameters) + interval '7' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m7, 
    
    max(case when surv_m8 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '8' month and (SELECT input_month FROM parameters) + interval '8' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m8, 
    
    max(case when surv_m9 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '9' month and (SELECT input_month FROM parameters) + interval '9' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m9, 
    
    max(case when surv_m10 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '10' month and (SELECT input_month FROM parameters) + interval '10' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m10, 
    
    max(case when surv_m11 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '11' month and (SELECT input_month FROM parameters) + interval '11' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m11, 
    
    max(case when surv_m12 = 1 and month_forward = (SELECT input_month FROM parameters) + interval '12' month and (SELECT input_month FROM parameters) + interval '12' month < (SELECT current_month FROM parameters) then max_total_mrc else null end) as mrc_m12
    
FROM forward_months_mrc A
LEFT JOIN survival B
    ON CAST(A.serviceno as varchar) = CAST(B.serviceno as varchar)
GROUP BY 1
)

--------------------------------------------------------------------------------
---------------------------------- FINAL RESULT ---------------------------
--------------------------------------------------------------------------------

--- Now we join all the information together and sort the final table.
--- Additionally, some columns needed for matching with the Fixed Sales Quality table
--- structure are added.

, final_result as (
SELECT
    distinct A.serviceno, 
    A.accountno, 
    date_trunc('month', date(A.sell_date)) as sell_month,
    A.sell_date, 
    A.activation_date,
    A.client_name,
    'Wireless' as techflag,
    '' as socioeconomic_seg,
    '' as movement_flag,
    'R' as customer_type_code,
    'Residencial' as customer_type_desc,
    -- A.province, --- Omitted beacuse the geographical hierarchy is being implemented directly in the dashboard.
    A.district, 
    A.procedencia, 
    A.sell_channel, 
    A.agent_acc_code, 
    A.sales_agent_name,
    A.activation_channel,
    A.handsets_flag,
    A.plan_code,
    A.plan_name,
    '' as mrc_plan,
    A.npn_30_flag,
    A.npn_60_flag,
    A.npn_90_flag, 
    A.npn_flag, 
    A.early_payment_flag,
    A.Payed_Entry_Fee_ammnt, 
    -- A.Payed_over_20_in_sell_month,
    surv_m0, surv_m1, surv_m2, surv_m3, surv_m4, surv_m5, surv_m6, surv_m7, surv_m8, surv_m9, surv_m10, surv_m11, surv_m12, 
    churn_m0, churn_m1, churn_m2, churn_m3, churn_m4, churn_m5, churn_m6, churn_m7, churn_m8, churn_m9, churn_m10, churn_m11, churn_m12, 
    
    -- invol_m0, invol_m1, invol_m2, invol_m3, invol_m4, invol_m5, invol_m6, invol_m7, invol_m8, invol_m9, invol_m10, invol_m11, invol_m12, 
    -- vol_m0, vol_m1, vol_m2, vol_m3, vol_m4, vol_m5, vol_m6, vol_m7, vol_m8, vol_m9, vol_m10, vol_m11, vol_m12, 
    
    churntype_m0, churntype_m1, churntype_m2, churntype_m3, churntype_m4, churntype_m5, churntype_m6, churntype_m7, churntype_m8, churntype_m9, churntype_m10, churntype_m11, churntype_m12,
    
    mrc_m0, mrc_m1, mrc_m2, mrc_m3, mrc_m4, mrc_m5, mrc_m6, mrc_m7, mrc_m8, mrc_m9, mrc_m10, mrc_m11, mrc_m12, 
    
    churn_m6 as churners_6_month, 
    '' as churners_90_1st_bill, --- We do not have a realiable column to get the issuance dt of the bills of the previous months.
    '' as churners_90_2nd_bill, 
    '' as churners_90_3rd_bill, 
    '' as rejoiners_1st_bill, 
    '' as rejoiners_2nd_bill, 
    '' as rejoiners_3rd_bill, 
    case when churn_m6 is not null and churntype_m6 = 'Voluntario' then 1 else null end as voluntary_churners_6_month,
    
    row_number() over (partition by A.serviceno order by sell_date asc) as r_nm --- For eliminating residual duplicates

FROM forward_months A
LEFT JOIN survival B
    ON A.serviceno = B.serviceno
LEFT JOIN churn C
    ON A.serviceno = C.serviceno
-- LEFT JOIN vol_churn D
    -- ON A.serviceno = D.serviceno
-- LEFT JOIN invol_churn E
    -- ON A.serviceno = E.serviceno
LEFT JOIN churn_type D
    ON A.serviceno = D.serviceno
LEFT JOIN mrc_evol F
    ON A.serviceno = F.serviceno
)

SELECT 
    *
FROM final_result
WHERE r_nm = 1 --- Eliminating residual duplicates

-- ORDER BY serviceno
-- LIMIT 10
