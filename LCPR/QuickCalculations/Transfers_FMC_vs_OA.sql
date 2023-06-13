WITH

parameters as (SELECT date('2023-04-01') as input_month)

, oa_transfers as (
SELECT
    cast(sub_acct_no_ooi as varchar) as account_id,
    bef_hsd + bef_video + bef_voice as churned_rgus
    -- aft_hsd + aft_video + aft_voice as aft_rgus
FROM "lcpr.sandbox.dev"."transactions_orderactivity"
WHERE 
    date_trunc('month', date(ls_chg_dte_ocr)) = (SELECT input_month FROM parameters) 
    and acct_type = 'R'
    and ord_typ = 'V_DISCO'
    and disco_rsn_sbb = 'VL'
-- LIMIT 10
)

, fmc_transfers as (
SELECT
    fix_s_att_account, 
    fix_b_mes_numrgus
    -- *
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters) 
    and fmc_b_att_active = 1
    and fix_s_fla_churntype = '3. Fixed Transfer'
)

, oa_fmc as (
SELECT
    case when A.account_id is not null and B.fix_s_att_account is not null then A.account_id end as common_account, 
    case when A.account_id is not null and B.fix_s_att_account is null then A.account_id end as just_oa_account, 
    case when A.account_id is null and B.fix_s_att_account is not null then B.fix_s_att_account end as just_fmc_account, 
    case when A.account_id is not null then 1 else 0 end as oa_dummy, 
    case when A.account_id is not null and B.fix_s_att_account is null then 1 else 0 end as just_oa_dummy, 
    case when B.fix_s_att_account is not null then 1 else 0 end as fmc_dummy, 
    case when A.account_id is null and B.fix_s_att_account is not null then 1 else 0 end as just_fmc_dummy,
    A.churned_rgus, 
    B.fix_b_mes_numrgus
FROM oa_transfers A
FULL OUTER JOIN fmc_transfers B
    ON cast(A.account_id as varchar) = cast(B.fix_s_att_account as varchar)
)

SELECT
    count(distinct common_account) as common_account, 
    count(distinct just_oa_account) as just_oa_accounts, 
    count(distinct just_fmc_account) as just_fmc_accounts, 
    sum(churned_rgus*oa_dummy*fmc_dummy) as common_rgus, 
    sum(churned_rgus*just_oa_dummy) as rgus_just_oa, 
    sum(fix_b_mes_numrgus*just_fmc_dummy) as rgus_just_fmc
FROM oa_fmc
    
-- SELECT * FROM fmc_transfers LIMIT 10
