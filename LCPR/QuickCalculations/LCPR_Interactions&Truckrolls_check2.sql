WITH

interactions as (
SELECT
    date(interaction_start_time) as start_date, 
    interaction_type_id, 
    interaction_id, 
    account_id
    -- count(distinct interaction_id) as num_interactions, 
    -- count(distinct account_id) as num_accounts
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE 
    date(interaction_start_time) > date('2023-06-20')
-- GROUP BY 1, 2
-- ORDER BY 1 asc, 2 asc
)

, truckrolls as (
SELECT 
    date(create_dte_ojb) as start_date, 
    job_no_ojb as truckroll_id, 
    sub_acct_no_sbb as account_id
FROM "lcpr.stage.dev"."truckrolls" 
WHERE 
    date(create_dte_ojb) > date('2023-06-20')
)

, joining as (
SELECT
    case when a.start_date is null then b.start_date else a.start_date end as start_date, 
    case when b.truckroll_id is not null then 'Truckroll' else 'Interaction' end as interaction_type_id, 
    case when b.truckroll_id is null then cast(a.interaction_id as varchar) else cast(b.truckroll_id as varchar) end as interaction_id, 
    case when b.account_id is null then cast(a.account_id as varchar) else cast(b.account_id as varchar) end as account_id
FROM interactions a
FULL OUTER JOIN truckrolls b
    ON a.start_date = b.start_date and cast(a.account_id as varchar) = cast(b.account_id as varchar)
)

SELECT
    distinct start_date, 
    interaction_type_id, 
    count(distinct interaction_id) as num_interactions, 
    count(distinct account_id) as num_accounts
FROM joining
GROUP BY 1, 2
ORDER BY 1 asc, 2 asc
