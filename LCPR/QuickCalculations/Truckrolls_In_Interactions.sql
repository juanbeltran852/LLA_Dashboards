WITH

interactions as (
SELECT 
    distinct date(interaction_start_time) as date_start,
    interaction_type_id,
    interaction_id, 
    account_id
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    date(interaction_start_time) between date('2023-04-29') and date('2023-05-07')
    
)

, truckrolls as (
SELECT 
    date(create_dte_ojb) as date_start, 
    job_no_ojb as interaction_id, 
    sub_acct_no_sbb as account_id
FROM "lcpr.stage.dev"."truckrolls" 
WHERE 
    date(create_dte_ojb) between date('2023-04-29') and date('2023-05-07')
)

, join_interactions_truckrolls as (
SELECT
    case when A.date_start is null then B.date_start else A.date_start end as date_start, 
    case when B.interaction_id is not null then 'Truckroll' else 'Interaction' end as type, 
    case when cast(B.interaction_id as varchar) is not null then cast(B.interaction_id as varchar) else cast(A.interaction_id as varchar) end as interaction_id, 
    case when cast(B.account_id as varchar) is not null then cast(B.account_id as varchar) else cast(A.account_id as varchar) end as account_id
FROM interactions A
FULL OUTER JOIN truckrolls B
    ON cast(A.account_id as varchar) = cast(B.account_id as varchar) and A.date_start = B.date_start
)

-- SELECT
--     distinct date_start, 
--     type,
--     count(distinct interaction_id) as num_interactions, 
--     count(distinct account_id) as num_accounts
-- -- FROM interactions
-- FROM join_interactions_truckrolls
-- GROUP BY 1, 2
-- ORDER BY 1, 2


SELECT
    distinct date_start, 
    type,
    count(distinct interaction_id) as num_interactions, 
    count(distinct account_id) as num_accounts
-- -- FROM interactions
FROM (SELECT *, concat(cast(date_start as varchar), account_id) as key FROM join_interactions_truckrolls)
WHERE
    key in (SELECT concat(cast(date(interaction_start_time) as varchar), cast(account_id as varchar)) FROM "db-stage-dev-lf"."interactions_lcpr")
GROUP BY 1, 2
ORDER BY 1, 2
