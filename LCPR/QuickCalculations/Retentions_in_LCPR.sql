WITH

parameters as (SELECT date_trunc('month', date('2023-01-01')) as input_month)

, disconnections as (
SELECT
    order_id, 
    date_trunc('month', date(completed_date)) as disco_month,
    date(order_start_date) as order_start_date, 
    date(completed_date) as completed_date, 
    cast(account_id as varchar) as completeddx_cust_id
    -- cast(lob_bb_count as int) as bb_churn, 
    -- cast(lob_tv_count as int) as tv_churn, 
    -- cast(lob_vo_count as int) as vo_churn, 
    -- cast((lob_bb_count + lob_tv_count + lob_vo_count) as int) as total_rgus_churn
FROM "lcpr.stage.prod"."so_hdr_lcpr"
WHERE 
    date_trunc('month', date(completed_date)) between (SELECT input_month FROM parameters) and (SELECT input_month FROM parameters) + interval '7' day
    and (order_type = 'V_DISCO' or lower(order_type) like '%dwn%')
    and account_type = 'RES'
    and order_status = 'COMPLETE'
)

, attempts_cc as (
SELECT
    interaction_id, 
    date_trunc('month', date(interaction_start_time)) as interaction_month,
    date(interaction_start_time) as interaction_start_time, 
    date(interaction_end_time) as interaction_end_time, 
    cast(account_id as varchar) as attempt_cust_id,
    other_interaction_info10, 
    interaction_purpose_descrip,
    case when other_interaction_info10 = 'Retained Customer' then account_id else null end as ret_candidate
    -- case when other_interaction_info10 = 'Retained Customer' then 'Retained' else 'Not_Retained' end as retention_flag
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    date_trunc('month', date(interaction_start_time)) = (SELECT input_month FROM parameters)
    and interaction_status = 'Closed'
    -- and (other_interaction_info10 in ('Retained Customer', /*'Retention',*/ 'Not Retained') or interaction_purpose_descrip in ('Retained Customer', /*'Retention',*/ 'Not Retained'))
    and other_interaction_info10 in ('Retained Customer'/*, 'Retention', 'Not Retained'*/)
)

, rets as (
SELECT
    A.attempt_cust_id, 
    A.other_interaction_info10, 
    case when D.order_start_date >= (A.interaction_start_time) then A.attempt_cust_id else null end as failed_ret
FROM attempts_cc A
LEFT JOIN disconnections D
    ON cast(A.attempt_cust_id as varchar) = cast(D.completeddx_cust_id as varchar)
)

SELECT
    count(distinct attempt_cust_id) as total_rets, 
    count(distinct failed_ret) as failed_rets
FROM rets
