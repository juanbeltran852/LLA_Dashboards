WITH

come_back_to_life_accounts as (
SELECT
    *
FROM "db_stage_dev"."lcpr_mob_apr2023"
WHERE mob_s_fla_mainmovement = '5.Come Back to Life'
)

, month_to_look_in as (
SELECT
    *
FROM "db_stage_dev"."lcpr_mob_mar2023"
)

SELECT
    distinct mob_s_fla_mainmovement, 
    count(distinct mob_s_att_account)
FROM month_to_look_in
WHERE 
    mob_s_att_account in (SELECT mob_s_att_account FROM come_back_to_life_accounts)
GROUP BY 1

-- SELECT
--     *
-- FROM month_to_look_in
-- LIMIT 10
