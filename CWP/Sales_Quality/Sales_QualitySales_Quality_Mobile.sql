--------------------------------------------------------------------------------
-------------------------- CWP - SALES QUALITY MOBILE --------------------------
--------------------------------------------------------------------------------
--- 23/06/2023

WITH

parameters as (
SELECT 
    date('2023-04-01') as input_month, 
    date_trunc('month', date('2023-05-01')) as current_month
),

-------------- GENERAL INFORMATION ------------------------

--- #### #### Gross adds

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

candidates_sales as ( ----- Early clients???
SELECT
    distinct TRIM(CAST(billableaccountno AS VARCHAR)) AS accountno, --Account CODE
    MIN(DATE(dt)) AS first_dt  --First appearance in DNA
FROM (
    SELECT     
        DATE(dt) AS dt,
        billableaccountno,
        MIN(DATE(dt)) OVER(PARTITION BY TRIM(CAST(billableaccountno AS VARCHAR))) AS first_dt_user
    FROM "db-analytics-prod"."tbl_postpaid_cwp"
    WHERE
        account_status in ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
        and category in ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
        and date_trunc('month', date(dt)) between (SELECT input_month FROM parameters) - interval '6' month and (SELECT input_month FROM Parameters) --- Use just dt and consider also 1st day of next month?
    )
WHERE date_trunc('month', first_dt_user) = (SELECT input_month FROM parameters)
GROUP BY billableaccountno
),

--- ### ### Postpaid DNA
useful_dna as (
SELECT
    date(dt) as dt,
    billableaccountno as accountno,
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
    *
FROM gross_adds A
INNER JOIN useful_dna B
    ON A.accountno = B.accountno
)

/* This part is about first bill generated. As we rely on the DRC file and the Polaris info is being tested, then I won't be using that column for the moment.

,bills_of_interest AS (
SELECT act_acct_cd,
    -- Usamos la fecha de la primera factura generada y no del oldes_unpaid_bill para no ser susceptibles a errores en el fi_outst_age o oldet_unpaid_bill
    DATE(TRY(FILTER(ARRAY_AGG(fi_bill_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1])) AS first_bill_created
FROM sales_base
GROUP BY act_acct_cd 
)
*/



SELECT * FROM info_gross
