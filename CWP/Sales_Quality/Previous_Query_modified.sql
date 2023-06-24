---------------------------------------------------
--           QUERY CREATED 14/06/2023            --
---------------------------------------------------
/*
input month should be current month and info of last 2 years is returned for all users
*/

WITH 
Parameters AS (
SELECT 
    DATE('2023-06-01') AS input_month
    -- '24' AS Life_span_dna,
    -- '3' AS max_npn_time
),
/*
Table from DNA 
billableaccountno es necesario para pagos
FILTER2 Extract first 6 months of life
FILTER2 bussiness track B2C and onyl extract first 6 months from dna
FILTER3 Clients whose first appearance is in last 24 Months so months a year ago have 12 months data
COLUMNS: serviceno, first_dt, last_dt, first_bill_created_dt,all_billing_dates,first_due_dt
*/

Info_Early_Clientes AS ( 
SELECT
    TRIM(CAST(billableaccountno AS VARCHAR)) AS accountno, --Account CODE 
    max(province) as province, 
    max(district) as district,
    -- first_value(province) over (partition by billableaccountno order by province) as province, --Province in DNA
    -- first_value(district) over (partition by billableaccountno order by province) as district, --District in DNA
    -- TRIM(CAST(serviceno AS VARCHAR)) AS accountno, --service CODE 
    MIN(DATE(dna.dt)) AS first_dt,  --First appearance in DNA
    MAX(DATE(dna.dt)) AS last_dt,   --Last appearance in DNA 
    --Aca puede ser el otro error que crea el descache, donde no hay fi_bill_dt_m0 se utiliza para el sell date la primera aparicion en el dna.
    CASE
        WHEN TRY(FILTER(ARRAY_AGG(dna.fi_bill_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1]) IS NULL THEN MIN(DATE(dna.dt))
        ELSE TRY(FILTER(ARRAY_AGG(dna.fi_bill_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1])
    END AS first_bill_created_dt,
    -- TRY(FILTER(ARRAY_AGG(dna.fi_bill_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1]) AS first_bill_created_dt, --First billing date
    TRY(FILTER(ARRAY_AGG(dna.fi_bill_due_dt_m0 ORDER BY DATE(dt)), x -> x IS NOT NULL)[1]) AS first_due_dt, --First due date
    TRY(FILTER(ARRAY_AGG(DISTINCT dna.fi_bill_dt_m0 ORDER BY dna.fi_bill_dt_m0), x -> x IS NOT NULL)) AS all_billing_dates, --all billing dates
    TRY(FILTER(ARRAY_AGG(DISTINCT dna.total_mrc_d ORDER BY dna.total_mrc_d), x -> x IS NOT NULL)) AS all_mrc_amnts, -- all distinct mrc amnts
    TRY(FILTER(ARRAY_AGG(DISTINCT dna.tot_inv_mo ORDER BY dna.tot_inv_mo), x -> x IS NOT NULL)) AS all_inv_amnts, --all distinct invoice amnt
    MAX(dna.total_mrc_d) as max_tot_mrc
FROM (
    SELECT     
        DATE(dt) AS dt,
        billableaccountno,
        MIN(DATE(dt)) OVER(PARTITION BY TRIM(CAST(billableaccountno AS VARCHAR)) ) AS first_dt_user, 
        DATE(fi_bill_dt_m0) AS fi_bill_dt_m0,
        DATE(fi_bill_due_dt_m0) AS fi_bill_due_dt_m0,
        CAST(total_mrc_d AS DOUBLE) AS total_mrc_d,
        CAST(tot_inv_mo AS DOUBLE) AS tot_inv_mo,
        account_status AS account_status,
        category AS category, 
        province as province, 
        district as district
    FROM   
        "db-analytics-prod"."tbl_postpaid_cwp"
    ) dna
--FILTERS1
WHERE DATE_DIFF('MONTH',dna.first_dt_user,dna.dt)<7
--FILTERS2
AND dna.account_status IN ('ACTIVE','RESTRICTED', 'GROSS_ADDS')
AND dna.category IN ('Consumer', 'Consumer Mas Control','Low Risk Consumer', 'CW Employees')
--FILTER3
-- AND first_dt_user BETWEEN  (SELECT input_month FROM Parameters) - INTERVAL (SELECT Life_span_dna from Parameters) MONTH AND (SELECT input_month FROM Parameters) + INTERVAL '1' MONTH  
AND first_dt_user BETWEEN  (SELECT input_month FROM Parameters) - INTERVAL '24' MONTH AND (SELECT input_month FROM Parameters) + INTERVAL '1' MONTH  
GROUP BY dna.billableaccountno--, province, district
),

Info_Gross_Ventas AS ( --Tabla Ventas que sale de gross adds
SELECT 
    TRIM(CAST(gross.account AS VARCHAR)) AS accountno,
    gross.service AS serviceno,
    DATE_PARSE(TRIM(gross.date),'%m/%d/%Y') AS sell_date, --Format example 05/27/2023
    gross.channel_resumen AS sell_channel,
    gross.procedencia AS procedencia,
    gross.agent_acc_code AS agent_acc_code,
    gross.plan_name AS plan_name
FROM "db-stage-prod"."gross_ads_movil_b2c_newversion" gross
-- LIMIT 1000
),
/*
Tabla Pagos --La tabla de "cwp-marketing"."pagos_movil_b2c_agregado_completo" no la usan en la otra query entonces es sospechoso
De tabla de pagos miramos los pagos hechos desde la primera factura por fecha
*/
Info_Pagos AS ( --Tabla Pagos
SELECT 
    CAST(early.accountno AS VARCHAR) AS accountno,
    early.first_bill_created_dt AS first_bill_created_dt,
    ARRAY_AGG(pmnts.payment_amt_local ORDER by dt) AS all_payments_amnt,
    ARRAY_AGG(pmnts.dt ORDER by dt) AS all_payments_dt,
    ELEMENT_AT(ARRAY_AGG(pmnts.dt ORDER by dt),-1) AS last_payment_dt,
    ELEMENT_AT(ARRAY_AGG(pmnts.dt ORDER by dt),1) AS first_payment_dt,

    ROUND(SUM(CAST(payment_amt_local AS DOUBLE)),2) AS total_payments_in_3_months, --Este debe incluir los 20 del anticipado
    ROUND(SUM(IF(DATE_DIFF('DAY', DATE(early.first_bill_created_dt), dt) < 30  ,CAST(pmnts.payment_amt_local AS DOUBLE), NULL)),2) AS total_payments_30_days,
    ROUND(SUM(IF(DATE_DIFF('DAY', DATE(early.first_bill_created_dt), dt) < 60 ,CAST(pmnts.payment_amt_local AS DOUBLE), NULL)),2) AS total_payments_60_days,
    ROUND(SUM(IF(DATE_DIFF('DAY', DATE(early.first_bill_created_dt), dt) < 90 ,CAST(pmnts.payment_amt_local AS DOUBLE), NULL)),2) AS total_payments_90_days
FROM 
    (
    SELECT 
        first_bill_created_dt AS first_bill_created_dt,
        first_dt AS first_dt,
        TRY_CAST(accountno AS INT) AS accountno

    FROM Info_Early_Clientes 
    ) early
    INNER JOIN
    (
    SELECT 
        TRY_CAST(account_id AS INT) AS accountno,
        DATE_PARSE(dt, '%Y-%m-%d') AS dt,
        CAST(payment_amt_local AS DOUBLE) AS payment_amt_local
    FROM "db-stage-prod"."payments_cwp" 
    ) pmnts
    -- ON pmnts.accountno=early.accountno AND DATE(DATE_PARSE(pmnts.dt,'%Y-%m-%d')) BETWEEN early.first_bill_created_dt - INTERVAL '45' DAY AND early.first_bill_created_dt + INTERVAL '3' MONTH
    ON TRY_CAST(pmnts.accountno AS INT)=TRY_CAST(early.accountno AS INT) 
-- WHERE pmnts.dt BETWEEN early.first_bill_created_dt - INTERVAL '45' DAY AND early.first_bill_created_dt + INTERVAL ( SELECT max_npn_time from Parameters) MONTH
WHERE pmnts.dt BETWEEN early.first_bill_created_dt - INTERVAL '45' DAY AND early.first_bill_created_dt + INTERVAL '3' MONTH
GROUP BY early.accountno,early.first_bill_created_dt
),

/*
Table from Payments and gross ads over 20 on same month as added
JOIN Info_Gross_Ventas table with payments per month per account
COLUMNS: accountno, pmnt_sell_month_ammnt, procedencia, sell_channel, sell_date_day, sell_date_month, prev_to_sell_date_month
prev_to_sell_date_month to compare existance in dna 
*/
Info_Pago_Adelantado AS (
SELECT 
    pmnts.accountno AS accountno,
    ROUND(pmnts.pmnt_amnt_month,2) AS pmnt_sell_month_ammnt,
    clients.procedencia AS procedencia,
    clients.sell_channel AS sell_channel,
    clients.agent_acc_code AS agent_acc_code,
    clients.plan_name AS plan_name,
    DATE_TRUNC('DAY' , clients.sell_date) AS sell_date_day,
    DATE_TRUNC('MONTH' , clients.sell_date) AS sell_date_month,
    DATE_TRUNC('MONTH' , clients.sell_date - INTERVAL '1' MONTH) AS prev_to_sell_date_month 

FROM ( --Amount pagado por mes por cliente
    SELECT 
        account_id AS accountno, --cliente
        SUM(TRY_CAST(payment_amt_local AS DOUBLE)) AS pmnt_amnt_month, --amount
        DATE_TRUNC('MONTH' , DATE_PARSE(dt, '%Y-%m-%d')) AS pmnt_month --mes
    FROM "db-stage-prod"."payments_cwp" 
    GROUP BY account_id, DATE_TRUNC('MONTH' , DATE_PARSE(dt, '%Y-%m-%d'))
    ) pmnts 
    -- JOIN
    INNER JOIN 
    (
    SELECT *
    FROM Info_Gross_Ventas
    -- WHERE sell_date != NULL
    ) clients 
        ON TRY_CAST(pmnts.accountno AS VARCHAR)=TRY_CAST(clients.accountno AS VARCHAR) AND pmnts.pmnt_month=DATE_TRUNC('MONTH' , clients.sell_date)

),
/*
Table from DRC table that accounts for voluntary churners
COLUMNS: accountno, first_drc_date
Some users have multiple drc dates reported , first one is selected
*/

-- DRC_table AS (
-- SELECT CAST(act_acct_cd AS VARCHAR) AS accountno,
--     ARRAY_AGG(DATE_PARSE(drc_period_final,'%Y-%m') ORDER BY DATE_PARSE(drc_period_final,'%Y-%m'))[1] AS first_drc_date
-- FROM "cwp-marketing"."drc_movil_new" 
-- GROUP BY act_acct_cd
-- ),

DRC_table as (
SELECT
    cast(accountno as varchar) as accountno, 
    -- array_agg(date_parse(dt, '%Y-%m') order by date_parse(dt, '%Y-%m'))[1] as first_drc_date
    array_agg(dt order by dt)[1] as first_drc_date
FROM "lla_cco_int_ext_prod"."cwp_mov_ext_derecognition"
GROUP BY accountno
),

Client_history AS (
SELECT *,
    CASE 
        WHEN pmnt_sell_month_ammnt = NULL THEN 0
        ELSE pmnt_sell_month_ammnt
    END AS Payed_Entry_Fee_ammnt,
    CASE 
        WHEN pmnt_sell_month_ammnt > 20 THEN 'Payed_over_20'
        ELSE 'No_payed_over_20' 
    END AS Payed_over_20_sell_month,
    CASE 
        WHEN DATE_DIFF('day',sell_date_day,first_dt) < 5 THEN 'Cliente Existente'
        ELSE 'Cliente Nuevo'
    END AS Cliente_Existente,
    CASE   
        WHEN total_payments_30_days IS NULL THEN accountno
        WHEN total_payments_30_days < max_tot_mrc THEN accountno 
        ELSE NULL 
    END AS npn_30_flag,
    CASE   
        WHEN total_payments_60_days IS NULL THEN accountno
        WHEN total_payments_60_days < max_tot_mrc THEN accountno 
        ELSE NULL 
    END AS npn_60_flag,
    CASE   
        WHEN total_payments_90_days IS NULL THEN accountno
        WHEN total_payments_90_days < max_tot_mrc THEN accountno 
        ELSE NULL 
    END AS npn_90_flag,
    CASE   
        WHEN total_payments_in_3_months IS NULL THEN accountno
        WHEN total_payments_in_3_months < max_tot_mrc THEN accountno 
        ELSE NULL 
    END AS npn_flag,
    CASE   
        WHEN first_drc_date IS NULL OR drc_accountno IS NULL THEN 'No DRC'
        ELSE 'DRC' 
    END AS drc_flag

FROM (
    SELECT
        --Early clients info
        early.accountno AS accountno,  --Account CODE 
        early.first_dt AS first_dt,  --First appearance in DNA
        early.last_dt AS last_dt,   --Last appearance in DNA 
        early.all_mrc_amnts AS all_mrc_amnts,
        early.all_inv_amnts AS all_inv_amnts,
        early.max_tot_mrc AS max_tot_mrc,
        -- early.all_billing_dates AS all_billing_dates,
        -- early.first_due_dt AS first_due_dt,
        -- early.serviceno AS serviceno,                            --Service code
        -- early.billableaccountno AS billableaccountno,           --Billable Account CODE
        early.province as province, 
        early.district as district,

        --Sales info
        adel.pmnt_sell_month_ammnt AS pmnt_sell_month_ammnt,
        adel.sell_date_month AS sell_date_month,
        adel.sell_date_day AS sell_date_day,
        adel.procedencia AS procedencia,
        adel.sell_channel AS sell_channel,
        adel.agent_acc_code AS agent_acc_code,
        adel.plan_name AS plan_name,
        -- adel.accountno,
        -- adel.prev_to_sell_date_month


        -- DRC info
        drc.first_drc_date as first_drc_date,
        drc.accountno AS drc_accountno,

        --Payments info
        pmnts.first_bill_created_dt,
        pmnts.first_payment_dt,
        pmnts.last_payment_dt,
        pmnts.total_payments_30_days AS total_payments_30_days,
        pmnts.total_payments_60_days AS total_payments_60_days,
        pmnts.total_payments_90_days AS total_payments_90_days,
        pmnts.total_payments_in_3_months AS total_payments_in_3_months
        -- pmnts.all_payments_amnt,
        -- pmnts.all_payments_dt,

    FROM 
        Info_Early_Clientes early 
        LEFT JOIN 
        Info_Pago_Adelantado adel
        ON early.accountno=adel.accountno
        LEFT JOIN
        Info_Pagos pmnts
        ON early.accountno=pmnts.accountno
        LEFT JOIN DRC_table drc
        ON early.accountno=drc.accountno
    )
),

SUMMARY_CLIENTS AS (
SELECT  
    -- Payed_over_20_sell_month,
    -- DATE_TRUNC('MONTH',first_bill_created_dt) AS First_month,
    -- sell_date_month,
    procedencia,
    -- sell_channel,
    -- drc_flag,
    agent_acc_code,
    COUNT(accountno) AS Total_count ,
    COUNT(DISTINCT npn_30_flag) AS Number_USERS_NPN30,
    COUNT(DISTINCT npn_60_flag) AS Number_USERS_NPN60,
    COUNT(DISTINCT npn_90_flag) AS Number_USERS_NPN90, 
    COUNT(DISTINCT npn_flag) AS Number_USERS_NPN3_months
FROM Client_history
GROUP BY  
    -- Payed_over_20_sell_month, 
    -- DATE_TRUNC('MONTH',first_bill_created_dt),
    -- sell_date_month,
    procedencia,
    agent_acc_code
    -- sell_channel,
    -- drc_flag
)


-- SELECT * FROM Info_Early_Clientes
-- SELECT * FROM Info_Pago_Adelantado
-- SELECT * FROM Info_Pagos
-- SELECT * FROM Info_Gross_Ventas
-- SELECT * FROM SUMMARY_CLIENTS
-- WHERE procedencia IN ('Port In', 'New', 'Prepaid')
SELECT * FROM Client_history
-- WHERE CAST(accountno AS VARCHAR)='2082620'
ORDER BY random(*)
LIMIT 5
