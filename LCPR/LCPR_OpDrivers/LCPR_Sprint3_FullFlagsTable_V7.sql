--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 3 - OPERATIONAL DRIVERS - FULL FLAGS TABLE ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

--- WARNING: Estimated runtime of 7 minutes.
--- December may take more than 20 minutes for some reason.

WITH

parameters as (SELECT date('2023-03-01') as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- FMC Table --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, fmc_table as (
SELECT
    *
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account,
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null
GROUP BY 1, 2
ORDER BY 3 desc
)

, fmc_table_adj as (
SELECT 
    F.*,
    records_per_user
FROM fmc_table F
LEFT JOIN repeated_accounts R
    ON F.fix_s_att_account = R.fix_s_att_account and F.fmc_s_dim_month = R.fmc_s_dim_month
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- New customers --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, new_customers_pre as (
SELECT
    date_trunc('month', date(dt)) as dna_month,
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by DATE(dt) DESC) as timestamp) as date) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account, 
    bill_from_dte_sbb, 
    --- The total MRC must be calculated summing up the charges for the different fixed services.
    (video_chrg + hsd_chrg + voice_chrg) as fi_tot_mrc_amt,
    delinquency_days,
    dt
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(CONNECT_DTE_SBB)) between ((SELECT input_month FROM parameters) - interval '3' month) and (SELECT input_month FROM parameters)
ORDER BY 1
)

-- , new_customers3m as (   
-- SELECT 
--     dna_month,
--     date_trunc('month', fix_b_att_maxstart) as install_month, 
--     fix_b_att_maxstart,  
--     fix_s_att_account as new_sales3m_flag,
--     fix_s_att_account, 
--     fi_tot_mrc_amt,
--     delinquency_days,
--     bill_from_dte_sbb, 
--     dt
-- FROM new_customers_pre
-- WHERE date_trunc('month', date(fix_b_att_maxstart)) = ((SELECT input_month FROM parameters) - interval '3' month)
-- )

, new_customers2m as (   
SELECT 
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account as new_sales2m_flag,
    fix_s_att_account
FROM new_customers_pre
WHERE date_trunc('month', date(fix_b_att_maxstart)) = ((SELECT input_month FROM parameters) - interval '2' month)
)

, new_customers as (   
SELECT 
    dna_month,
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account as new_sales_flag,
    fix_s_att_account, 
    fi_tot_mrc_amt,
    delinquency_days,
    bill_from_dte_sbb, 
    dt
FROM new_customers_pre
WHERE date_trunc('month', date(fix_b_att_maxstart)) = (SELECT input_month FROM parameters)
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Interactions and truckrolls --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    cast(interaction_start_time as varchar) != ' ' 
    and interaction_start_time is not null
    and date_trunc('month', date(interaction_start_time)) between (SELECT input_month FROM parameters) and ((SELECT input_month FROM parameters) + interval '2' month)
    and account_type = 'RES'
)

, interactions_fields as (
SELECT
    *, 
    cast(substr(cast(interaction_start_time as varchar), 1, 10) as date) as interaction_date, 
    date_trunc('month', cast(substr(cast(interaction_start_time as varchar), 1, 10) as date)) as month
FROM clean_interaction_time
)

, interactions_not_repeated as (
SELECT
    first_value(interaction_id) OVER(PARTITION BY account_id, interaction_date, interaction_channel, interaction_agent_id, interaction_purpose_descrip ORDER BY interaction_date DESC) AS interaction_id2
FROM interactions_fields
)

, interactions_fields2 as (
SELECT *
FROM interactions_not_repeated a
LEFT JOIN interactions_fields b
    ON a.interaction_id2 = b.interaction_id
)

, truckrolls as (
SELECT 
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls" 
WHERE 
    date_trunc('month', date(create_dte_ojb)) between (SELECT input_month FROM parameters) and ((SELECT input_month FROM parameters) + interval '2' month)
)

, full_interactions as (
SELECT 
    *, 
    case 
    
        when create_dte_ojb is not null then 'truckroll'
    
        when (
        lower(interaction_purpose_descrip) like '%ppv%problem%'
        or lower(interaction_purpose_descrip) like '%hsd%problem%'
        or lower(interaction_purpose_descrip) like '%cable%problem%'
        or lower(interaction_purpose_descrip) like '%tv%problem%'
        or lower(interaction_purpose_descrip) like '%video%problem%'
        or lower(interaction_purpose_descrip) like '%tel%problem%'
        or lower(interaction_purpose_descrip) like '%phone%problem%'
        or lower(interaction_purpose_descrip) like '%int%problem%'
        or lower(interaction_purpose_descrip) like '%line%problem%'
        or lower(interaction_purpose_descrip) like '%hsd%issue%'
        or lower(interaction_purpose_descrip) like '%ppv%issue%'
        or lower(interaction_purpose_descrip) like '%video%issue%'
        or lower(interaction_purpose_descrip) like '%tel%issue%'
        or lower(interaction_purpose_descrip) like '%phone%issue%'
        or lower(interaction_purpose_descrip) like '%int%issue%'
        or lower(interaction_purpose_descrip) like '%line%issue%'
        or lower(interaction_purpose_descrip) like '%cable%issue%'
        or lower(interaction_purpose_descrip) like '%tv%issue%'
        or lower(interaction_purpose_descrip) like '%bloq%'
        or lower(interaction_purpose_descrip) like '%slow%'
        or lower(interaction_purpose_descrip) like '%slow%service%'
        or lower(interaction_purpose_descrip) like '%service%tech%'
        or lower(interaction_purpose_descrip) like '%tech%service%'
        or lower(interaction_purpose_descrip) like '%no%service%'
        or lower(interaction_purpose_descrip) like '%hsd%no%'
        or lower(interaction_purpose_descrip) like '%hsd%slow%'
        or lower(interaction_purpose_descrip) like '%hsd%intermit%'
        or lower(interaction_purpose_descrip) like '%no%brows%'
        or lower(interaction_purpose_descrip) like '%phone%cant%'
        or lower(interaction_purpose_descrip) like '%phone%no%'
        or lower(interaction_purpose_descrip) like '%no%connect%'
        or lower(interaction_purpose_descrip) like '%no%conect%'
        or lower(interaction_purpose_descrip) like '%no%start%'
        or lower(interaction_purpose_descrip) like '%equip%'
        or lower(interaction_purpose_descrip) like '%intermit%'
        or lower(interaction_purpose_descrip) like '%no%dat%'
        or lower(interaction_purpose_descrip) like '%dat%serv%'
        or lower(interaction_purpose_descrip) like '%int%data%'
        or lower(interaction_purpose_descrip) like '%tech%'
        or lower(interaction_purpose_descrip) like '%supp%'
        or lower(interaction_purpose_descrip) like '%outage%'
        or lower(interaction_purpose_descrip) like '%mass%'
        or lower(interaction_purpose_descrip) like '%discon%warn%'
        ) and (
        lower(interaction_purpose_descrip) not like '%work%order%status%'
        and lower(interaction_purpose_descrip) not like '%default%call%wrapup%'
        and lower(interaction_purpose_descrip) not like '%bound%call%'
        and lower(interaction_purpose_descrip) not like '%cust%first%'
        and lower(interaction_purpose_descrip) not like '%audit%'
        and lower(interaction_purpose_descrip) not like '%eq%code%'
        and lower(interaction_purpose_descrip) not like '%downg%'
        and lower(interaction_purpose_descrip) not like '%upg%'
        and lower(interaction_purpose_descrip) not like '%vol%discon%'
        and lower(interaction_purpose_descrip) not like '%discon%serv%'
        and lower(interaction_purpose_descrip) not like '%serv%call%'
        )
        then 'tech_call'
        
        else null
        
        end as interaction_type
        
FROM interactions_fields2 a
FULL OUTER JOIN truckrolls b
    ON a.interaction_date = cast(create_dte_ojb as date) and cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar)
WHERE
    interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- Never Paid (using Payments Table as in CWP) --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

,bills_of_interest AS (
SELECT 
    dna_month,
    fix_s_att_account,
    --- I take the equivalent to oldest_unpaid_bill because no much more alternative info is available
    first_value(bill_from_dte_sbb) over (partition by fix_s_att_account order by dt asc) as first_bill_created
FROM new_customers
)

, mrc_calculation as (
SELECT
    nc.fix_s_att_account, 
    min(first_bill_created) as first_bill_created, 
    max(fi_tot_mrc_amt) as max_tot_mrc, 
    array_agg(distinct fi_tot_mrc_amt order by fi_tot_mrc_amt desc) as arreglo_mrc
FROM new_customers nc
INNER JOIN bills_of_interest bi
    ON nc.fix_s_att_account = bi.fix_s_att_account 
        and date(nc.dt) between date(first_bill_created) and (date(first_bill_created) + interval '3' month)
GROUP BY nc.fix_s_att_account
)

, first_cycle_info as (
SELECT
    nc.fix_s_att_account, 
    --nc.dna_month, 
    min(fix_b_att_maxstart) as first_installation_date, 
    min(first_bill_created) as first_bill_created, 
    try(array_agg(arreglo_mrc)[1]) as arreglo_mrc, 
    max(delinquency_days) as max_delinquency_days_first_bill, 
    max(max_tot_mrc) as max_mrc_first_bill, 
    count(distinct max_tot_mrc) as diff_mrc
FROM new_customers nc
INNER JOIN mrc_calculation mrcc
    ON nc.fix_s_att_account = mrcc.fix_s_att_account
WHERE 
    date(nc.bill_from_dte_sbb) = date(mrcc.first_bill_created)
GROUP BY nc.fix_s_att_account--, nc.dna_month
)

, payments_basic as (
SELECT  
    account_id as fix_s_att_account_payments, 
    try(array_agg(date(dt) order by date(dt))[1]) as first_payment_date, 
    try(array_agg(cast(payment_amt_usd as double) order by date(dt))[1]) as first_payment_amt, 
    try(filter(array_agg(case when cast(payment_amt_usd as double) >= max_mrc_first_bill then date(dt) else null end order by date(dt)), x -> x is not null)[1]) as first_payment_above_date, 
    try(filter(array_agg(case when cast(payment_amt_usd as double) >= max_mrc_first_bill then cast(payment_amt_usd as double) else null end order by date(dt)), x -> x is not null)[1]) as first_payment_above, 
    array_agg(cast(payment_amt_usd as double) order by date(dt)) as arreglo_pagos, 
    try(array_agg(date(dt) order by date(dt))[1]) as first_pay_date, 
    try(array_agg(date(dt) order by date(dt) desc)[1]) as last_pay_date, 
    array_agg(date(dt) order by date(dt)) as arreglo_pagos_dates, 
    
    round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<30, cast(payment_amt_usd as double),null)),2) as total_payments_30_days
    ,round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<60, cast(payment_amt_usd as double),null)),2) as total_payments_60_days
    ,round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<85, cast(payment_amt_usd as double),null)),2) as total_payments_85_days
    
FROM "lcpr.stage.prod"."payments_lcpr" p
INNER JOIN first_cycle_info as fc
    ON cast(fc.fix_s_att_account as varchar) = cast(p.account_id as varchar)
WHERE 
    date(dt) between (fc.first_bill_created - interval '50' day) and (fc.first_bill_created + interval '85' day)
GROUP BY account_id
)

, npn_85 as (
SELECT
    *, 
    fc.fix_s_att_account as fix_s_att_account_def,
    date_diff('day', first_bill_created, first_payment_above_date) as days_between_payment, 
    case when first_payment_above_date is null then 86 else date_diff('day', first_bill_created, first_payment_above_date) end as fixed_days_unpaid_bill, 
    case
        when total_payments_30_days is null then fc.fix_s_att_account
        when total_payments_30_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_30_flag,
    
    case
        when total_payments_60_days is null then fc.fix_s_att_account
        when total_payments_60_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_60_flag,
    case
        when total_payments_85_days is null then fc.fix_s_att_account
        when total_payments_85_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_85_flag
FROM first_cycle_info fc
LEFT JOIN payments_basic p 
    ON cast(p.fix_s_att_account_payments as varchar) = cast(fc.fix_s_att_account as varchar)
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Early tickets --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, relevant_interactions as (
SELECT
    customer_id, 
    interaction_id, 
    job_no_ojb,
    interaction_type,
    min(interaction_date) as min_interaction_date, 
    min(date_trunc('month', date(interaction_date))) as interaction_start_month 
FROM full_interactions
GROUP BY 1, 2, 3, 4
)

, early_tickets AS (
SELECT 
    A.fix_s_att_account, 
    new_sales2m_flag,
    install_month, 
    interaction_start_month, 
    fix_b_att_maxstart,
    case when date_diff('week', date(fix_b_att_maxstart), date(min_interaction_date)) <= 7 then fix_s_att_account else null end as early_ticket_flag
FROM new_customers2m A 
LEFT JOIN relevant_interactions B 
    ON cast(A.fix_s_att_account as varchar) = cast(B.customer_id as varchar)
WHERE interaction_type in ('tech_call', 'truckroll')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Outlier Installs --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, installations as (
SELECT
    *
FROM "lcpr.stage.prod"."so_ln_lcpr"
WHERE
    org_id = 'LCPR' and org_cntry = 'PR'
    and order_status = 'COMPLETE'
    and command_id = 'CONNECT'
)

, outlier_installs as (
SELECT
    fix_s_att_account, 
    fix_b_att_maxstart, 
    new_sales_flag,
    install_month,
    cast(cast(order_start_date as timestamp) as date) as order_start_date, 
    cast(cast(completed_date as timestamp) as date) as completed_date, 
    case when date_diff('day', date(order_start_date), date(completed_date)) > 6 then fix_s_att_account else null end as outlier_install_flag
FROM new_customers a
LEFT JOIN installations b
    ON cast(a.fix_s_att_account as varchar) = cast(b.account_id as varchar)
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- MRC Changes --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, previous_plan as (
SELECT
    sub_acct_no_sbb, 
    first_value(play_type) over (partition by sub_acct_no_sbb, dt order by dt asc) as previous_plan_bom
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE
    date_trunc('month', date(dt) ) = (SELECT input_month FROM parameters) - interval '1' month
    and play_type != '0P'
    and cust_typ_sbb = 'RES' 
)

, current_plan as (
SELECT
    sub_acct_no_sbb, 
    first_value(play_type) over (partition by sub_acct_no_sbb, dt order by dt desc) as current_plan_eom
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE
    date_trunc('month', date(dt) ) = (SELECT input_month FROM parameters)
    and play_type != '0P'
    and cust_typ_sbb = 'RES' 
)

, same_plan_customers as (
SELECT
    C.sub_acct_no_sbb, 
    P.previous_plan_bom,
    C.current_plan_eom as unchanged_plan
FROM current_plan C
INNER JOIN previous_plan P
    ON C.sub_acct_no_sbb = P.sub_acct_no_sbb
WHERE 
    previous_plan_bom = current_plan_eom
)

, bom_previous_month as (
SELECT 
    fmc_s_dim_month as fmc_s_dim_month_prev, 
    fix_s_att_account, 
    fmc_s_att_account, 
    fix_b_mes_mrc as fix_b_mes_mrc_prev
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters) - interval '1' month  
    and fix_b_mes_overdue < 85
    and fix_s_att_account in (SELECT sub_acct_no_sbb FROM same_plan_customers)
 )

, eom_current_month as (
SELECT
    fmc_s_dim_month, 
    fix_s_att_account, 
    fix_e_mes_mrc
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters) 
    and fix_e_mes_overdue < 85
    and fix_s_att_account in (SELECT sub_acct_no_sbb FROM same_plan_customers)
)

 , mrc_changes as (
SELECT  
    fmc_s_dim_month,
    c.fix_s_att_account,
    case when fix_e_mes_mrc/fix_b_mes_mrc_prev > 1.05 then c.fix_s_att_account else null end as MRC_increase_flag,
    case when fix_e_mes_mrc/fix_b_mes_mrc_prev <= 1.05 then c.fix_s_att_account else null end as no_plan_change
FROM bom_previous_month p 
LEFT JOIN eom_current_month c 
    ON p.fix_s_att_account = c.fix_s_att_account and p.fmc_s_dim_month_prev + interval '1' month = c.fmc_s_dim_month
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Billing Claims --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, pre_bill_claim as (
SELECT 
    customer_id, 
    interaction_start_time, 
    date_trunc('month', date(interaction_start_time)) as interaction_start_month, 
    interaction_purpose_descrip
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE 
    interaction_purpose_descrip in ('Adjustment Request', 'Approved Adjustment', 'Billing', 'Cancelled Np', 'Chuito Retained', 'Vd: Billing', 'Vd: Cant Afford', 'Vd: Closed Buss', 'Vd: Deceased', 'E-Bill', 'G:customer Billable', 'Not Retained', 'Np: Cancelled Np', 'Np: Payment Plan', 'Np: Promise To Pay', 'Promise To Pay', 'Ret- Adjustment', 'Ret- Promise-To-Pay', 'Ret-Bill Expln', 'Ret-Direct Debit', 'Ret-Pay Meth Expln', 'Ret-Payment', 'Ret-Right Pricing', 'Rt: Price Increase', 'Rt: Rate Pricing')
    or (interaction_purpose_descrip like '%Ci:%')
    or (interaction_purpose_descrip like '%Payment%')
    or interaction_purpose_descrip like '%Vd: Can%'
    and (account_type = 'RES') 
    and (interaction_status = 'Closed')
)

, bill_claim as (
SELECT 
    *, 
    customer_id as bill_claim_flag
FROM pre_bill_claim
WHERE 
    interaction_purpose_descrip not in ('Ci: Cable Card Req', 'Ci: Inst/Tc Status', 'Ci: Install Stat', 'Ci: Installer / Tech')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Mounting Bills --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, mounting_bills as (
SELECT  
    date_trunc('month', date(dt)) as fmc_s_dim_month,
    delinquency_days, 
    sub_acct_no_sbb as fix_s_att_account, 
    case when delinquency_days = 60 then sub_acct_no_sbb else null end as mounting_bill_flag
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type <> '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(dt)) = (SELECT input_month FROM parameters)
GROUP BY 1, 2, 3
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- ,Never_paid_fmc as (
-- select 
--     a.*, CASE WHEN npn_flag is not null THEN 1 ELSE null END AS NEVER_PAID_FLAG,npn_flag from new_customers_flag a left join summary_by_user b on a.finalaccount = b.act_acct_cd
-- )

, flag_new_customers as (
SELECT
    F.*, 
    new_sales_flag
FROM fmc_table_adj F
LEFT JOIN new_customers I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar)
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    -- and fix_e_att_active = 1
)

, flag_npn_85 as (
SELECT
    F.*, 
    npn_30_flag,
    npn_60_flag,
    npn_85_flag
FROM flag_new_customers F
LEFT JOIN npn_85 I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account_def as varchar)
)

, flag_early_tickets as (
SELECT
    F.*, 
    early_ticket_flag
FROM flag_npn_85 F
LEFT JOIN early_tickets I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar)
)

, flag_outlier_installs as (
SELECT
    F.*,
    outlier_install_flag
FROM flag_early_tickets F
LEFT JOIN outlier_installs I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar)
)

, flag_mrc_changes as (
SELECT
    F.*, 
    MRC_increase_flag, 
    no_plan_change
FROM flag_outlier_installs F
LEFT JOIN mrc_changes I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar) 
)

, flag_billing_claims as (
SELECT
    F.*, 
    bill_claim_flag
FROM flag_mrc_changes F
LEFT JOIN bill_claim I
    ON cast(F.fix_s_att_account as varchar) = cast(I.customer_id as varchar) and F.fmc_s_dim_month = I.interaction_start_month
)

, flag_mounting_bills as (
 SELECT 
    F.*,
    mounting_bill_flag
FROM flag_billing_claims F 
LEFT JOIN mounting_bills I 
    ON F.fmc_s_dim_month = I.fmc_s_dim_month and F.fix_s_att_account = I.fix_s_att_account 
 )

, final_table as (
SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech,
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment,
    fmc_e_fla_fmc as odr_e_fla_fmc_type,
    case 
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure,
    count(distinct case when fix_e_att_active = 1 then fix_s_att_account else null end) as odr_s_mes_active_base,
    count(distinct fix_s_att_account) as odr_s_mes_total_accounts,
    count(distinct new_sales_flag) as opd_s_mes_sales,
    count(distinct npn_30_flag) as opd_s_mes_never_paid_30_days,
    count(distinct npn_60_flag) as opd_s_mes_never_paid_60_days,
    count(distinct npn_85_flag) as opd_s_mes_never_paid_85_days,  
    count(distinct early_ticket_flag) as opd_s_mes_uni_early_tickets,
    count(distinct outlier_install_flag) as opd_s_mes_long_installs, 
    count(distinct MRC_Increase_flag) as opd_s_mes_uni_mrcincrease, 
    count(distinct no_plan_change) as opd_s_mes_uni_mrcnoincrease, 
    count(distinct bill_claim_flag) as opd_s_mes_uni_bill_claim, 
    count(distinct mounting_bill_flag) as opd_s_mes_uni_moun_gbills
FROM flag_mounting_bills
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
)

SELECT * FROM final_table

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tests --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

--- --- --- ### ### ### Never Paid (85 days)

-- SELECT 
--     sum(opd_s_mes_never_paid) as npn_85, 
--     sum(opd_s_mes_sales) as sales_base, 
--     cast(sum(opd_s_mes_never_paid) as double)/cast(sum(opd_s_mes_sales) as double) as KPI
-- FROM final_table

--- --- --- ### ### ### Early Tickets (7 weeks)

-- SELECT 
--     sum(opd_s_mes_uni_early_tickets) as early_tickets, 
--     sum(opd_s_mes_sales) as sales_base, 
--     cast(sum(EarlyTickets) as double)/cast(sum(opd_s_mes_sales) as double) as KPI
-- FROM final_table

--- --- --- ### ### ### Outlier Installs (6 days)

-- SELECT
--     sum(opd_s_mes_long_installs) as outlier_installs, 
--     sum(opd_s_mes_sales) as sales_base, 
--     cast(sum(opd_s_mes_long_installs) as double)/cast(sum(opd_s_mes_sales) as double) as KPI
-- FROM final_table

--- --- --- ### ### ### MRC Changes (5% increase)

-- SELECT 
--     sum(opd_s_mes_uni_mrcincrease) as MRC_increase, 
--     sum(opd_s_mes_uni_mrcnoincrease) as No_change, 
--     cast(sum(opd_s_mes_uni_mrcincrease) as double)/cast(sum(opd_s_mes_uni_mrcnoincrease) as double) as KPI
-- FROM final_table

--- --- --- ### ### ### Billing Claims

-- SELECT 
--     sum(opd_s_mes_uni_bill_claim) as bill_claims, 
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(opd_s_mes_uni_bill_claim) as double)/cast(sum(odr_s_mes_active_base) as double) as KPI 
-- FROM final_table

--- --- --- ### ### ### Mounting Bills

-- SELECT 
--     sum(opd_s_mes_uni_moun_gbills) as mounting_bills, 
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(opd_s_mes_uni_moun_gbills) as double)/cast(sum(odr_s_mes_active_base) as double) as KPI
-- FROM final_table
