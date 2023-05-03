-- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 4.2 - VOLUNTARY CHURN EQUATION ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

WITH

parameters as (SELECT date_trunc('month', date('2023-03-01')) as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- FMC --- --- --- --- --- --- --- --- --- --- --- ---
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
--- --- --- --- --- --- --- --- Disconnections (Service Orders approach) --- --- --- --- --- --- --- 
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- diss as (
-- 	SELECT account_number as diss_id,
-- 		lower(disconnected_services) as diss_services,
-- 		date_trunc('Month', date_parse(service_end_dt, '%m/%d/%Y')) as mth,
-- 		department as department,
-- 		CASE
-- 			WHEN lower(disconnected_services) = 'Click' THEN 'BO'
-- 			when lower(disconnected_services) = 'watch' THEN 'TV'
-- 			when lower(disconnected_services) = 'Talk' THEN 'VO'
-- 			when lower(disconnected_services) = 'NA' THEN 'NA' --HAY NA
-- 			when lower(disconnected_services) = 'na,click' THEN 'BO' --HAY NA
-- 			when lower(disconnected_services) = 'Watch,click' THEN 'BO+TV'
-- 			when lower(disconnected_services) = 'talk,click' THEN 'BO+VO'
-- 			when lower(disconnected_services) = 'Watch,talk' THEN 'VO+TV'
-- 			when lower(disconnected_services) = 'Watch,talk,click ' THEN 'BO+VO+TV'
-- 			when lower(disconnected_services) = 'talk,mobile,click' THEN 'BO+VO' --HAY MOBILE
-- 		end as dis_mixname
-- 	FROM "lla_cco_int_ext_dev"."cwc_ext_disconnections"
-- 	where disconnected_services <> 'MOBILE'
-- ),

, disconnections as (
SELECT
    order_id, 
    date_trunc('month', date(completed_date)) as disco_month,
    date(order_start_date) as order_start_date, 
    date(completed_date) as completed_date, 
    account_id, 
    cast(lob_bb_count as int) as bb_churn, 
    cast(lob_tv_count as int) as tv_churn, 
    cast(lob_vo_count as int) as vo_churn, 
    cast((lob_bb_count + lob_tv_count + lob_vo_count) as int) as total_rgus_churn
FROM "lcpr.stage.prod"."so_hdr_lcpr"
WHERE 
    order_type = 'V_DISCO'
    and account_type = 'RES'
    and order_status = 'COMPLETE'
    and org_id = 'LCPR'
    and org_cntry = 'PR'
    and date_trunc('month', date(order_start_date)) = (SELECT input_month FROM parameters)

)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- ---  Retentions (Interactions approach) --- --- --- --- --- --- --- --- 
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, retentions as (
SELECT
    interaction_id, 
    date_trunc('month', date(interaction_start_time)) as interaction_month,
    date(interaction_start_time) as interaction_start_time, 
    date(interaction_end_time) as interaction_end_time, 
    account_id,
    interaction_purpose_descrip
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    date_trunc('month', date(interaction_start_time)) = (SELECT input_month FROM parameters)
    and interaction_status = 'Closed'
    and other_interaction_info10 ='Retained Customer'
    -- and other_interaction_info10 in ('Retained Customer', 'Retention', 'Not Retained')
    -- and (other_interaction_info10 = 'Retained Customer' or interaction_purpose_descrip = 'Retained Customer')
)

, actual_retentions as (
SELECT
    interaction_month as retention_month, 
    case when order_start_date between (interaction_start_time) and (interaction_start_time + interval '3' day) then null else I.account_id end as actual_retentions
FROM retentions I
LEFT JOIN disconnections D
    ON cast(I.account_id as varchar) = cast(D.account_id as varchar) and (I.interaction_month = D.disco_month)
)

SELECT retention_month, count(distinct actual_retentions) FROM actual_retentions GROUP BY 1
