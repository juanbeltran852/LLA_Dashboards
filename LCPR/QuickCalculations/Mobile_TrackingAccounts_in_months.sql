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
WHERE
    mob_s_att_account in (SELECT mob_s_att_account FROM cbacktlife_customers)
)

, m_2 as (
SELECT
    *
FROM "db_stage_dev"."lcpr_mob_feb2023"
WHERE
    mob_s_att_account in (SELECT mob_s_att_account FROM m_1)
)

, m_3 as (
SELECT
    *
FROM "db_stage_dev"."lcpr_mob_jan2023"
WHERE
    mob_s_att_account in (SELECT mob_s_att_account FROM m_2)
ORDER BY mob_s_att_account asc
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
FULL OUTER JOIN m_1 B
    ON A.mob_s_att_account = B.mob_s_att_account
FULL OUTER JOIN m_2 C
    ON A.mob_s_att_account = C.mob_s_att_account
FULL OUTER JOIN m_3 D
    ON A.mob_s_att_account = D.mob_s_att_account
)

SELECT
    *
FROM tracking
WHERE
    m_1 is not null
    and m_2 is not null
    and m_3 is not null
    
