WITH

parameters as (SELECT date('2023-05-01') as input_month) ---- Must be the first day of the month.

-------------------------------------------------------------------------------
---------------------------------- MSTR TABLE ---------------------------------
-------------------------------------------------------------------------------

--- --- ### Old

SELECT
    count(distinct subsrptn_id) as num_subs, 
    count(distinct cust_id) as num_parent
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE
    date(dt) = (SELECT input_month FROM parameters)
    -- and cust_sts = 'O'
    -- and acct_type_cd = 'I'
    -- and rgn_nm <> 'VI'
    -- and subsrptn_sts = 'A'

--- --- ### New

-- SELECT
--     count(distinct subscription_id) as num_subs,
--     count(distinct customer_id) as num_parent
-- FROM "transform-lcpr-stage-dev"."att_dna_pstpd"
-- WHERE
--     date(dt) = (SELECT input_month FROM parameters)
--     -- and customer_status = 'O'
--     -- and acct_type_cd = 'I'
--     -- and region_name <> 'VI'
--     -- and subscription_status = 'A'

    

-------------------------------------------------------------------------------
---------------------------------- INCR TABLE ---------------------------------
-------------------------------------------------------------------------------

/* Daily comparisons are not possible. The volume check must be made on a month level.*/

--- --- ### Old

-- SELECT
--     count(distinct subsrptn_id) as num_subs, 
--     count(distinct cust_id) as num_parent
-- FROM "lcpr.stage.dev"."tbl_pstpd_cust_cxl_incr_data"
-- WHERE
--     date(dt) = (SELECT input_month FROM parameters)
--     and acct_type_cd = 'I'
--     and rgn_nm <> 'VI'
--     /* Unvalid disco reasons */
--     -- and lower(acct_sts_rsn_desc) not like '%contract%accepted%' and lower(acct_sts_rsn_desc) not like '%portin%' and lower(acct_sts_rsn_desc) not like '%ctn%activation%' and lower(acct_sts_rsn_desc) not like '%per%cust%req%' and lower(acct_sts_rsn_desc) not like '%reduced%rate%suspend%' and lower(acct_sts_rsn_desc) not like '%""%' and lower(lst_susp_rsn_desc) not like '%""%'
--     /* Check for involuntary */
--     -- and (lower(acct_sts_rsn_desc) LIKE '%no%pay%' or lower(acct_sts_rsn_desc) LIKE '%no%use%' or lower(acct_sts_rsn_desc) LIKE '%fraud%' or lower(acct_sts_rsn_desc) LIKE '%off%net%' or lower(acct_sts_rsn_desc) LIKE '%pay%def%' or lower(acct_sts_rsn_desc) LIKE '%lost%equip%' or lower(acct_sts_rsn_desc) LIKE '%tele%conv%' or lower(acct_sts_rsn_desc) LIKE '%cont%acce%req%' or lower(acct_sts_rsn_desc) LIKE '%proce%' or lower(lst_susp_rsn_desc) LIKE '%no%pay%' or lower(lst_susp_rsn_desc) LIKE '%no%use%' or lower(lst_susp_rsn_desc) LIKE '%fraud%' or lower(lst_susp_rsn_desc) LIKE '%off%net%' or lower(lst_susp_rsn_desc) LIKE '%pay%def%' or lower(lst_susp_rsn_desc) LIKE '%lost%equip%' or lower(lst_susp_rsn_desc) LIKE '%tele%conv%' or lower(lst_susp_rsn_desc) LIKE '%proce%')


--- --- ### New

-- SELECT
--     count(distinct A.subscription_id) as num_subs, 
--     count(distinct A.customer_id) as num_parent
-- FROM "transform-lcpr-stage-dev"."att_dna_prepd_sbp" A
-- /* A left join between new tables is needed beacuse 2 important columns used in the old table are not in the equivalent new table */
-- LEFT JOIN (SELECT subscription_id, customer_id, lst_susp_rsn_desc, acct_type_cd FROM "transform-lcpr-stage-dev"."att_dna_pstpd" WHERE date(dt) = (SELECT input_date FROM parameters)) B
-- ON A.subscription_id = B.subscription_id
-- WHERE
--     date(dt) = (SELECT input_month FROM parameters)
--     and acct_type_cd = 'I'
--     and ba_region_name <> 'VI'
--     /* Unvalid disco reasons */
--     and lower(sts_rsn_desc) not like '%contract%accepted%' and lower(sts_rsn_desc) not like '%portin%' and lower(sts_rsn_desc) not like '%ctn%activation%' and lower(sts_rsn_desc) not like '%per%cust%req%' and lower(sts_rsn_desc) not like '%reduced%rate%suspend%' and lower(sts_rsn_desc) not like '%""%' and lower(lst_susp_rsn_desc) not like '%""%'
--     /* Check for involuntary */
--     and (lower(sts_rsn_desc) LIKE '%no%pay%' or lower(sts_rsn_desc) LIKE '%no%use%' or lower(sts_rsn_desc) LIKE '%fraud%' or lower(sts_rsn_desc) LIKE '%off%net%' or lower(sts_rsn_desc) LIKE '%pay%def%' or lower(sts_rsn_desc) LIKE '%lost%equip%' or lower(sts_rsn_desc) LIKE '%tele%conv%' or lower(sts_rsn_desc) LIKE '%cont%acce%req%' or lower(sts_rsn_desc) LIKE '%proce%' or lower(lst_susp_rsn_desc) LIKE '%no%pay%' or lower(lst_susp_rsn_desc) LIKE '%no%use%' or lower(lst_susp_rsn_desc) LIKE '%fraud%' or lower(lst_susp_rsn_desc) LIKE '%off%net%' or lower(lst_susp_rsn_desc) LIKE '%pay%def%' or lower(lst_susp_rsn_desc) LIKE '%lost%equip%' or lower(lst_susp_rsn_desc) LIKE '%tele%conv%' or lower(lst_susp_rsn_desc) LIKE '%proce%')

-------------------------------------------------------------------------------
---------------------------------- ERC TABLE ---------------------------------
-------------------------------------------------------------------------------

--- --- ### Old

-- SELECT
--     count(distinct subsrptn_id) as num_subs, 
--     count(distinct cust_id) as num_parent
-- FROM "lcpr.stage.dev"."tbl_prepd_erc_cust_mstr_ss_data"
-- WHERE
--     date(dt) = (SELECT input_month FROM parameters) --- Ideally, must be a BOM or EOM day.
--     -- AND cust_sts = 'O'
--     -- AND acct_type_cd = 'I'
--     -- AND ba_rgn_nm <> 'VI'
--     -- AND subsrptn_sts = 'A'


--- --- ### New

-- SELECT
--     count(distinct subscription_id) as num_subs,
--     count(distinct customer_id) as num_parent
-- FROM "transform-lcpr-stage-dev"."att_dna_prepd_erc"
-- WHERE
--     date(dt) = (SELECT input_month FROM parameters)
--     -- and customer_status = 'O'
--     -- and acct_type_cd = 'I'
--     -- and region_name <> 'VI'
--     -- and subscription_status = 'A'

