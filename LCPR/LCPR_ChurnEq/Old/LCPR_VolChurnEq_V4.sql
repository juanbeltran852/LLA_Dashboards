-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4.2 - VOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2023-01-01')) as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- FMC - Total Voluntary Churn --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, total_vol_churn as (
SELECT
    *,
    cast(fix_s_att_account as varchar) as vol_churn_id,
    fmc_s_fla_churntype as churntype
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fmc_s_fla_churntype = 'Voluntary Churner'
    -- and cast(fix_s_att_account as varchar) not like '%-%'
)
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- ---  Disconnection orders (Service Orders approach) --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

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
    date_trunc('month', date(order_start_date)) = (SELECT input_month FROM parameters)
    and (order_type = 'V_DISCO' or lower(order_type) like '%dwn%')
    -- and order_type = 'V_DISCO'
    and account_type = 'RES'
    and order_status = 'COMPLETE'
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- ---  Customers that went through CC (Interactions approach) --- --- --- --- --- --- 
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, attempts_cc as (
SELECT
    interaction_id, 
    date_trunc('month', date(interaction_start_time)) as interaction_month,
    date(interaction_start_time) as interaction_start_time, 
    date(interaction_end_time) as interaction_end_time, 
    cast(account_id as varchar) as attempt_cust_id,
    other_interaction_info10, 
    interaction_purpose_descrip,
    case when other_interaction_info10 = 'Retained Customer' then 'Retained' else 'Not_Retained' end as retention_flag
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    date_trunc('month', date(interaction_start_time)) = (SELECT input_month FROM parameters)
    and interaction_status = 'Closed'
    and (other_interaction_info10 in ('Retained Customer', /*'Retention',*/ 'Not Retained') or interaction_purpose_descrip in ('Retained Customer', /*'Retention',*/ 'Not Retained'))
    -- and other_interaction_info10 in ('Retained Customer', 'Retention', 'Not Retained')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- ---  Voluntary Churn Equation Flags --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, vol_churn_eq as (
SELECT
    V.vol_churn_id, 
    D.completeddx_cust_id, 
    C.attempt_cust_id, 
    case 
        when D.completeddx_cust_id is not null and C.attempt_cust_id is null then D.completeddx_cust_id
        when D.completeddx_cust_id is null and C.attempt_cust_id is not null then C.attempt_cust_id
        when D.completeddx_cust_id is not null and C.attempt_cust_id is not null then C.attempt_cust_id
    end as all_attempts_cust_flag, 
    case when D.completeddx_cust_id is not null and C.attempt_cust_id is null then D.completeddx_cust_id else null end as not_cc_cust_flag, 
    
    case when C.retention_flag = 'Retained' and D.completeddx_cust_id is null then C.attempt_cust_id else null end as ret_cust_flag,
    case when C.attempt_cust_id is not null and D.completeddx_cust_id is not null then C.attempt_cust_id else null end as notret_cust_flag,
    case when C.attempt_cust_id is not null and D.completeddx_cust_id is null then C.attempt_cust_id else null end as notcompleted_dx_allcc_flag, ---!!!!
    
    -- case when C.retention_flag = 'Retained' then C.attempt_cust_id else null end as ret_cust_flag, 
    -- case when C.retention_flag = 'Not_Retained' then C.attempt_cust_id else null end as notret_cust_flag, 
    -- case when C.retention_flag = 'Not_Retained' and D.completeddx_cust_id is not null then D.completeddx_cust_id else null end as completed_dx_cc_flag, 
    -- case when C.retention_flag = 'Not_Retained' and D.completeddx_cust_id is null then C.attempt_cust_id else null end as notcompleted_dx_cc_flag,
    
    case when V.vol_churn_id is not null and D.completeddx_cust_id is null then D.completeddx_cust_id end as bajas_no_cursadas 
    
    -- case when V.vol_churn_id is null and C.retention_flag = 'Not_Retained' then C.attempt_cust_id end as weird_notret
    
FROM total_vol_churn V
FULL OUTER JOIN disconnections D
    ON V.vol_churn_id = D.completeddx_cust_id
FULL OUTER JOIN attempts_cc C
    ON (D.completeddx_cust_id = C.attempt_cust_id) --0 and D.order_start_date >= C.interaction_start_time)
)

SELECT 
    count(distinct vol_churn_id) as vol_churn, 
    count(distinct completeddx_cust_id) as completed_dx, 
    count(distinct attempt_cust_id) as attempts_cc, 
    count(distinct all_attempts_cust_flag) as all_attempts, 
    count(distinct not_cc_cust_flag) as not_through_cc, 
    count(distinct ret_cust_flag) as retained_cust, 
    count(distinct notret_cust_flag) as notretained_cust, 
    -- count(distinct completed_dx_cc_flag) as completed_dx_cc, 
    count(distinct notcompleted_dx_allcc_flag) as notcompleted_dx_all_cc, 
    count(distinct bajas_no_cursadas) as bajas_no_cursadas 
    -- count(distinct weird_notret) as not_ret_remained
FROM vol_churn_eq
