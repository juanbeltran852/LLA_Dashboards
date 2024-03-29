--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - REPEATED CALLERS #####

--- ### Initial steps

WITH
  
 parameters as (
 SELECT date_trunc('month', date('2023-01-01')) as input_month --- Input month you wish the code run for
 )



, fmc_table as ( --- This actually is the Fixed Table, it is called fmc just to get ready for when that table is ready
SELECT
    fix_s_dim_month, --- month
    fix_b_fla_tech, --- B_Final_TechFlag
    fix_b_fla_fmc, --- B_FMCSegment
    fix_b_fla_mixcodeadj, --- B_FMCType
    fix_e_fla_tech, --- E_Final_Tech_Flag
    fix_e_fla_fmc, --- E_FMCSegment
    fix_e_fla_mixcodeadj, --- E_FMCType
    fix_b_fla_tenure, -- b_final_tenure
    fix_e_fla_tenure, --- e_final_tenure
    --- B_FixedTenure
    --- E_FixedTenure
    --- finalchurnflag
    fix_s_fla_churntype, --- fixedchurntype
    fix_s_fla_churnflag, --- fixedchurnflag
    fix_s_fla_mainmovement, --- fixedmainmovement
    --- waterfall_flag
    --- finalaccount
    fix_s_att_account, -- fixedaccount
    fix_e_att_active --- f_activebom
    --- mobile_activeeom
    --- mobilechurnflag
FROM "db_stage_dev"."lcpr_fixed_table_jan_mar17" --- Keep this updated to the lastest version!
WHERE 
    fix_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fix_s_dim_month, 
    fix_s_att_account, 
    count(*) as records_per_user
FROM fmc_table
GROUP BY 1, 2
ORDER BY 3 desc
)

, fmc_table_adj as (
SELECT 
    F.*,
    records_per_user
FROM fmc_table F
LEFT JOIN repeated_accounts R
    ON F.fix_s_att_account = R.fix_s_att_account and F.fix_s_dim_month = R.fix_s_dim_month
)

, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', cast(substr(cast(interaction_start_time as varchar),1,10) as date)) between ((SELECT input_month FROM parameters)) and ((SELECT input_month FROM parameters) + interval '1' month)
    and account_type = 'RES'
)

, interactions_fields as (
SELECT
    *, 
    cast(substr(cast(interaction_start_time as varchar), 1, 10) as date) as interaction_date, 
    date_trunc('month', cast(substr(cast(interaction_start_time as varchar), 1, 10) as date)) as month
FROM clean_interaction_time
)

--- ### Repeated callers

, last_interaction as (
SELECT 
    account_id as last_account, 
    first_value(interaction_date) over (partition by account_id, date_trunc('month', interaction_date) order by interaction_date desc) as last_interaction_date
FROM interactions_fields
)

, join_last_interaction as (
SELECT
    account_id, 
    interaction_id, 
    interaction_date, 
    date_trunc('month', last_interaction_date) as interaction_month, 
    last_interaction_date, 
    date_add('DAY', -60, last_interaction_date) as window_day
FROM interactions_fields W
INNER JOIN last_interaction L
    ON W.account_id = L.last_account
)

, interactions_count as (
SELECT
    interaction_month, 
    account_id, 
    count(distinct interaction_id) as interactions
FROM join_last_interaction
WHERE
    interaction_date between window_day and last_interaction_date
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

--- ### Repeated callers flag

, interaction_tier_flag as(
SELECT 
    F.*, 
    case when I.account_id is not null then F.fix_s_att_account else null end as interactions, 
    interaction_tier
FROM fmc_table_adj F
LEFT JOIN interactions_tier I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fix_s_dim_month = I.interaction_month
)

, final_fields as (
SELECT
    distinct fix_s_dim_month as opd_s_dim_month, -- month
    fix_b_fla_tech as fmc_b_fla_tech_type, -- B_Final_TechFlag
    fix_b_fla_fmc as fmc_b_fla_fmc_status, -- B_FMCSegment
    fix_b_fla_mixcodeadj as fmc_b_dim_mix_code_adj, -- B_FMCType
    fix_e_fla_tech as fmc_e_fla_tech_type, -- E_Final_TechFlag
    fix_e_fla_fmc as fmc_e_fla_fmc_status, -- E_FMCSegment
    fix_e_fla_mixcodeadj as fmc_e_dim_mix_code_adj, -- E_FMCType
    -- b_final_tenure
    -- e_final_tenure
    fix_b_fla_tenure as fmc_b_fla_final_tenure, -- B_FixedTenure
    fix_e_fla_tenure as fmc_e_fla_final_tenure, -- E_FixedTenure
    -- finalchurnflag
    -- fixedchurnflag
    fix_s_fla_churntype as fmc_s_fla_churn_type, -- fixedchurntype
    fix_s_fla_mainmovement as fmc_s_fla_main_movement, -- fixedmainmovement
    -- waterfall_flag
    -- mobile_activeeom
    -- mobilechurnflag
    interaction_tier as odr_s_fla_interaction_tier,
    -- finalaccount
    fix_s_att_account as fmc_s_att_final_account, -- fixedaccount
    interactions as odr_s_mes_user_interactions,
    records_per_user
FROM interaction_tier_flag
WHERE fix_s_fla_churnflag = '2. Fixed NonChurner'
    and fix_e_att_active = 1
)

SELECT
    opd_s_dim_month, -- fix_s_dim_month, -- month
    fmc_b_fla_tech_type, -- fix_b_fla_tech, -- B_Final_TechFlag
    fmc_b_fla_fmc_status, -- fix_b_fla_fmc, -- B_FMCSegment
    fmc_b_dim_mix_code_adj -- fix_b_fla_mixcodeadj, -- B_FMCType
    fmc_e_fla_tech_type, -- fix_e_fla_tech, -- E_Final_TechFlag
    fmc_e_fla_fmc_status, -- fix_e_fla_fmc, -- E_FMCSegment
    fmc_e_dim_mix_code_adj, -- fix_e_fla_mixcodeadj, -- E_FMCType
    -- b_final_tenure
    -- e_final_tenure
    fmc_b_fla_final_tenure, -- fix_b_fla_tenure, -- B_FixedTenure
    fmc_e_fla_final_tenure, -- fix_e_fla_tenure, -- E_FixedTenure
    -- finalchurnflag
    -- fixedchurnflag
    -- waterfall_flag
    odr_s_fla_interaction_tier, -- interaction_tier,
    count(distinct fmc_s_att_final_account) as odr_s_mes_total_accounts
    -- count(distinct fmc_s_att_final_account) as Fixed_Accounts
FROM final_fields
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

--- ### Specific numbers

-- SELECT
--     count(distinct fmc_s_att_final_account) as num_cliets
-- FROM final_fields
-- WHERE
--     interaction_tier = '1'
