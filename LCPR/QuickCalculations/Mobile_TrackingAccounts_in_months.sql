WITH

cbacktlife_customers as (
SELECT
    *
FROM "db_stage_dev"."lcpr_mob_apr2023"
WHERE
    mob_s_fla_mainmovement = '5.Come Back to Life'
    and mob_e_mes_tenuredays < 0
)

, m_1 as (
SELECT
    *
FROM "db_stage_dev"."lcpr_mob_mar2023"
-- WHERE
    -- mob_s_att_account in (SELECT mob_s_att_account FROM cbacktlife_customers)
)

, m_2 as (
SELECT
    *
FROM "db_stage_dev"."lcpr_mob_feb2023"
-- WHERE
    -- mob_s_att_account in (SELECT mob_s_att_account FROM m_1)
)

, m_3 as (
SELECT
    *
FROM "db_stage_dev"."lcpr_mob_jan2023"
-- WHERE
    -- mob_s_att_account in (SELECT mob_s_att_account FROM m_2)
)

, tracking as (
SELECT
    A.*, 
    D.mob_s_att_account as m_3, 
    D.mob_s_fla_mainmovement as m_3_mainmovement, 
    C.mob_s_att_account as m_2,
    C.mob_s_fla_mainmovement as m_2_mainmovement,
    B.mob_s_att_account as m_1,
    B.mob_s_fla_mainmovement as m_1_mainmovement,
    A.mob_s_att_account as m_0, 
    A.mob_s_fla_mainmovement as m_0_mainmovement
FROM cbacktlife_customers A
LEFT JOIN m_1 B
    ON A.mob_s_att_account = B.mob_s_att_account
LEFT JOIN m_2 C
    ON A.mob_s_att_account = C.mob_s_att_account
LEFT JOIN m_3 D
    ON A.mob_s_att_account = D.mob_s_att_account
)

, month_track as (
SELECT
    *, 
    case 
        when (m_1 is not null and m_2 is not null and m_3 is not null) then 'm_0_only'
        when (m_1 is not null and m_2 is null and m_3 is null) then 'm_1'
        when (m_1 is null and m_2 is not null and m_3 is null) then 'm_2'
        when (m_1 is null and m_2 is null and m_3 is not null) then 'm_3'
        when (m_1 is not null and m_2 is not null and m_3 is null) then 'm_1&m_2'
        when (m_1 is not null and m_2 is null and m_3 is not null) then 'm_1&m_3'
        when (m_1 is null and m_2 is not null and m_3 is not null) then 'm_2&m_3'
        when (m_1 is not null and m_2 is not null and m_3 is not null) then 'm_1&m_2&m_3'
    end as track_flag
FROM tracking
-- WHERE
--     m_1 is not null
--     and m_2 is not null
--     and m_3 is not null
)

SELECT
    distinct track_flag, 
    count(distinct mob_s_att_account) as num_accounts
FROM month_track
GROUP BY 1

