------------------------------------------------------------------------------------------------------------------------------

-- LCPR - Interactions detailed by Technology:

--        FTTH, HFC3.0, HFC3.1, and HFC-OTHERS as default value for cases where is unidentifiable

--        the technology of the Account

--------------------------------------------------------------------------------------------------------------------------------

 

WITH dna_fixed_lcpr AS

(


    SELECT  CASE WHEN REGEXP_LIKE(UPPER(cpe), 'D3.1') THEN 'HFC3.1'

                 WHEN UPPER(REPLACE(drop_type, ' ', '')) IN ('COAX', 'FIBCO') THEN 'HFC3.0'

                 WHEN REGEXP_LIKE(UPPER(drop_type), 'FIBER|FTTH') THEN 'FTTH'

                 ELSE 'HFC-OTHERS'

            END technology_type,

            DATE(as_of) as_of,

            CAST(sub_acct_no_sbb AS VARCHAR) sub_acct_no_sbb,

            drop_type, cpe, hsd, hsd_speed, hsd_upload_speed, play_type, hub_tv, voice, cable_cards

    FROM "db-stage-prod-lf"."insights_customer_services_rates_lcpr"

    WHERE 1 = 1

    AND CAST(DATE(as_of) AS VARCHAR) = '2023-05-31'

    AND UPPER(play_type) <> '0P'    

    AND UPPER(cust_typ_sbb) = 'RES'

    AND VIP_FLG_SBB NOT IN ('S','C','H','L','O','B')

)

 

SELECT dna.technology_type,

       inte.*

FROM "db-stage-prod-lf"."interactions_lcpr" inte

LEFT JOIN dna_fixed_lcpr dna

  ON inte.account_id =dna.sub_acct_no_sbb

  AND DATE(inte.interaction_start_time) = dna.as_of

  AND UPPER(inte.interaction_status) = 'CLOSED'

  AND UPPER(inte.account_type) = 'RES'

WHERE 1 = 1

  AND DATE(inte.interaction_start_time) = DATE('2023-05-31')

  AND REGEXP_LIKE(lower(interaction_purpose_descrip),'no.+service|intermit|issue|bloqueo.+red|hsd.+slow.+service|intermit|tech|conection|^return.+equip|no.+browsing|no.+signal|outage|hsd.+issue|bloq|slow|no.+service|no.+data')

  AND regexp_like(lower(other_interaction_info10),'phone|contact.+center|service.+problems|bloq.+red|whatsaap')
