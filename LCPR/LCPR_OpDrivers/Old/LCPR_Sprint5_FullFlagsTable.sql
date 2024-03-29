--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - FULL FLAGS TABLE #####

--- ### ### ### Initial steps (Common in most of the calculations)

WITH

--- --- --- Month you wish the code run for
parameters as (SELECT date_trunc('month', date ('2023-02-01')) as input_month)

--- --- --- FMC table
, fmc_table as (
SELECT
    *
FROM "db_stage_dev"."lcpr_fmc_table_dec_mar23" 
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23")
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_feb_mar23")
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account,
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null
    and fix_e_att_active = 1
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

--- --- --- Interactions
, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', date(interaction_start_time)) between (SELECT input_month FROM parameters) - interval '2' month and (SELECT input_month FROM parameters)
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
SELECT 
    *, 
    date_trunc('month', interaction_date) as interaction_month
FROM interactions_not_repeated a
LEFT JOIN interactions_fields b
    ON a.interaction_id2 = b.interaction_id
)

--- --- --- External file: Truckrolls
, truckrolls as (
SELECT 
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls"
)


--- ### ### ### ### ### REPEATED CALLERS ### ### ### ### ###

--- % users with one call
--- Num: Customers with 1 call
--- Denom: Active base

--- % users with 2 calls
--- Num: Customers with 2 calls
--- Denom: Active base

--- % users with 3 or more calls
--- Num: Customers with 3 or more calls
--- Denom: Active base

--- This KPI requires us to check the Interactions Table for the last 2 months. However, the denominator is the active base in the current month, which we obtain from the FMC.

, interactions_count_pre as (
SELECT
    account_id,
    interaction_id,
    interaction_month, 
    interaction_date,
    first_value(interaction_date) over(partition by account_id, date_trunc('month', interaction_date) order by interaction_date desc) as last_interaction_date
FROM interactions_fields2
)

, interactions_count as (
SELECT
    interaction_month, 
    account_id, 
    count(distinct interaction_id) as interactions
FROM interactions_count_pre
WHERE
    interaction_date between date_add('day', -60, last_interaction_date) and interaction_date --- This is the Moving Window
GROUP BY 1, 2
)

, interactions_tier as (
SELECT
    *, 
    case 
        when interactions = 1 then '1'
        when interactions = 2 then '2'
        when interactions >= 3 then '>3'
        else null
    end as interaction_tier
FROM interactions_count
)


--- ### ### ### ### ### REITERATIVE TICKETS ### ### ### ### ###

--- % users with one ticket
--- Num: Customers with one ticket
--- Denom: Active base

--- % users with two tickets
--- Num: Customers with two tickets
--- Denom: Active base

--- % users with three or more tickets
--- Num: Customers with 3 or more tickets
--- Denom: Active base

--- Again, we need to check the Interactions Table for the last 2 months, but in this case we are focusing just in interactions associated to a tech ticket.

, users_tickets_pre as (
SELECT
    distinct account_id, 
    interaction_id, 
    interaction_date,
    interaction_month,
    case when (
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
        then interaction_id else null
    end as techticket_flag,
    cast(job_no_ojb as varchar) as truckroll_flag
FROM interactions_fields2 a
LEFT JOIN truckrolls b
    ON a.interaction_date = cast(create_dte_ojb as date) and cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar)
WHERE
     interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')
    and (lower(interaction_purpose_descrip) like '%ppv%problem%'
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
)

, users_tickets as (
SELECT 
    *, 
    case 
        when techticket_flag is null and truckroll_flag is null then null
        when techticket_flag is not null and truckroll_flag is null then interaction_id
        when techticket_flag is null and truckroll_flag is not null then interaction_id
        when techticket_flag is not null and truckroll_flag is not null then interaction_id
    end as number_tickets
FROM users_tickets_pre
)

, last_ticket as (
SELECT 
    account_id as last_account, 
    first_value(interaction_date) over(partition by account_id, date_trunc('month', interaction_date) order by interaction_date desc) as last_interaction_date
FROM users_tickets
)

, join_last_ticket as (
SELECT
    account_id, 
    interaction_id, 
    interaction_date, 
    interaction_month, 
    last_interaction_date,
    number_tickets
FROM users_tickets W
INNER JOIN last_ticket L
    ON W.account_id = L.last_account
)

, tickets_count as (
SELECT 
    interaction_month, 
    account_id, 
    count(distinct number_tickets) as tickets
FROM join_last_ticket
WHERE interaction_date between date_add('day', -60, last_interaction_date) and last_interaction_date --- This is the Moving Window
GROUP BY 1, 2
)

, tickets_tier as (
SELECT 
    *,
    case
        when tickets = 1 then '1'
        when tickets = 2 then '2'
        when tickets >= 3 then '>3'
    else null end as ticket_tier
FROM tickets_count
)


--- ### ### ### ### ### OUTLIER REPAIR TIMES ### ### ### ### ###

--- Skipped for now.

--- ### ### ### ### ### MISSED VISITS ### ### ### ### ###

--- Skipped for now.

--- ### ### ### ### ### TICKETS PER MONTH ### ### ### ### ###

--- Num: Total tickets 
--- Denom: Active base

--- For this KPI we check the interactions associated to tickets and count how many of them we had in the current month.


, tickets_per_month as (
SELECT
    date_trunc('month', interaction_date) as month, 
    account_id, 
    count(distinct 
        case 
            when techticket_flag is null and truckroll_flag is null then null
            when techticket_flag is not null and truckroll_flag is null then interaction_id
            when techticket_flag is null and truckroll_flag is not null then interaction_id
            when techticket_flag is not null and truckroll_flag is not null then interaction_id
        end)
    as number_tickets
FROM users_tickets
WHERE
    interaction_id is not null
GROUP BY 1, 2
)

--- ### ### ### ### ### NODES TICKET DENSITY ### ### ### ### ###

--- It is in another script to match with the CX Table Structure

--- ### ### ### ### ### JOINING ALL FLAGS ### ### ### ### ###

, flag1_repeated_callers as(
SELECT 
    F.*, 
    case when I.account_id is not null then F.fix_s_att_account else null end as interactions, 
    interaction_tier
FROM fmc_table_adj F
LEFT JOIN interactions_tier I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.interaction_month
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fix_e_att_active = 1 
)

, flag2_reiterative_tickets as (
SELECT 
    F.*, 
    ticket_tier
FROM flag1_repeated_callers F
LEFT JOIN tickets_tier I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.interaction_month
)

, flag3_tickets_per_month as (
SELECT
    F.*, 
    number_tickets
FROM flag2_reiterative_tickets F 
LEFT JOIN tickets_per_month I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.month
)


--- ### ### ### ### ### FINAL TABLE ### ### ### ### ###

--- --- --- Jamaica's structure
, sprint5_full_table_LikeJam as (
SELECT 
    fmc_s_dim_month as odr_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech, -- E_Final_Tech_Flag, 
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment, -- E_FMC_Segment, 
    fmc_e_fla_fmc as odr_e_fla_fmc_type, -- E_FMCType, 
    case 
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure, ---E_FinalTenureSegment,
    interaction_tier as odr_s_fla_interaction_tier, 
    ticket_tier as odr_s_fla_tickets_tier, 
    count(distinct fix_s_att_account) as odr_s_mes_active_base, -- as activebase, 
    count(distinct interactions) as odr_s_mes_user_interactions,
    count(distinct case when ticket_tier = '1' then fix_s_att_account else null end) as odr_s_mes_one_ticket, -- as one_ticket,  
    count(distinct case when ticket_tier in ('2', '>3') then fix_s_att_account else null end) as odr_s_mes_over1_ticket, -- as over1_ticket, 
    count(distinct case when ticket_tier = '2' then fix_s_att_account else null end) as odr_s_mes_two_tickets, -- as two_tickets, 
    count(distinct case when ticket_tier = '>3' then fix_s_att_account else null end) as odr_s_mes_three_more_tickets, -- as three_more_tickets,
    sum(number_tickets) as odr_s_mes_total_tickets 
    -- count(distinct outlier_repair) as outlier_repairs 
FROM flag3_tickets_per_month
WHERE
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', '6.Null last day', 'Churn Exception')
    and fix_s_fla_mainmovement != '6.Null last day' --- Be careful! This is not the final mainmovement flag.
GROUP BY 1, 2, 3, 4, 5, 6, 7    
ORDER BY 1, 2, 3, 4, 5, 6
)


--- --- ---
SELECT * FROM sprint5_full_table_LikeJam

--- ### ### ### Specific numbers

--- --- --- Repeated callers
-- SELECT
--     odr_s_fla_interaction_tier,
--     sum(odr_s_mes_active_base) as num_cliets
-- FROM sprint5_full_table_LikeJam
-- GROUP BY 1

--- --- --- Reiterative tickets
-- SELECT 
--     odr_s_fla_tickets_tier,
--     sum(odr_s_mes_active_base) as num_cliets
-- FROM sprint5_full_table_LikeJam
-- GROUP BY 1

--- --- --- Tickets per month
-- SELECT
--     sum(odr_s_mes_total_tickets) as number_tickets,
--     sum(odr_s_mes_active_base) as active_base, 
--     round(cast(sum(odr_s_mes_total_tickets) as double)/(cast(sum(odr_s_mes_active_base) as double)/100), 2) as tickets_per_100_users
-- FROM sprint5_full_table_LikeJam
