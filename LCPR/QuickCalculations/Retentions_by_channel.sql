WITH

parameters as (SELECT date('2022-12-01') as input_month)

, all_attempts as (
SELECT
    B.interaction_id,
    B.account_id as ret_account_id,
    B.other_interaction_info10, 
    B.interaction_channel,
    date(interaction_start_time) as interaction_start_time
FROM "lcpr.stage.prod"."lcpr_interactions_csg" B
WHERE
    -- date_trunc('month', date(B.interaction_start_time)) = (date('2022-12-01'))
    B.interaction_status = 'Closed'
    and B.other_interaction_info10 in ('Retained Customer'/*, 'Retention'*/, 'Not Retained')
)

, december as (
SELECT
    distinct interaction_channel, 
    count(distinct interaction_id) as num_ints
FROM all_attempts
WHERE
    date_trunc('month', date(interaction_start_time)) = (date('2022-12-01'))
GROUP BY 1
ORDER BY 1
-- LIMIT 10
)

, january as (
SELECT
    distinct interaction_channel, 
    count(distinct interaction_id) as num_ints
FROM all_attempts
WHERE
    date_trunc('month', date(interaction_start_time)) = (date('2023-01-01'))
GROUP BY 1
ORDER BY 1
-- LIMIT 10
)

, february as (
SELECT
    distinct interaction_channel, 
    count(distinct interaction_id) as num_ints
FROM all_attempts
WHERE
    date_trunc('month', date(interaction_start_time)) = (date('2023-02-01'))
GROUP BY 1
ORDER BY 1
-- LIMIT 10
)

, march as (
SELECT
    distinct interaction_channel, 
    count(distinct interaction_id) as num_ints
FROM all_attempts
WHERE
    date_trunc('month', date(interaction_start_time)) = (date('2023-03-01'))
GROUP BY 1
ORDER BY 1
-- LIMIT 10
)

, april as (
SELECT
    distinct interaction_channel, 
    count(distinct interaction_id) as num_ints
FROM all_attempts
WHERE
    date_trunc('month', date(interaction_start_time)) = (date('2023-04-01'))
GROUP BY 1
ORDER BY 1
-- LIMIT 10
)

SELECT
    case
        when a.interaction_channel is not null then a.interaction_channel
        when b.interaction_channel is not null then b.interaction_channel
        when c.interaction_channel is not null then c.interaction_channel
        when d.interaction_channel is not null then d.interaction_channel
    else e.interaction_channel end as interaction_channel, 
    sum(a.num_ints) as December2022, 
    sum(b.num_ints) as January2023,
    sum(c.num_ints) as February2023,
    sum(d.num_ints) as March2023,
    sum(e.num_ints) as April2023
FROM december a
FULL OUTER JOIN january b
    ON a.interaction_channel = b.interaction_channel
FULL OUTER JOIN february c
    ON a.interaction_channel = c.interaction_channel or b.interaction_channel = c.interaction_channel
FULL OUTER JOIN march d
    ON a.interaction_channel = d.interaction_channel or b.interaction_channel = d.interaction_channel or c.interaction_channel = d.interaction_channel
FULL OUTER JOIN april e
    ON a.interaction_channel = e.interaction_channel or b.interaction_channel = e.interaction_channel or c.interaction_channel = e.interaction_channel or d.interaction_channel = e.interaction_channel
GROUP BY 1
ORDER BY 1 asc
