--============================================================================================================================
-- Sales Quality Dashboard - Grouped table for 6th month waterfall analysis
-- Input: The input for this table is the table generated by the mobile sales quality code.
-- Usage: Once the 12 months of analysis have been executed with the mobile sales quality code, you can run this code to generate
-- the required table for the waterfall sections of the dashboard.
-- ===========================================================================================================================
SELECT sell_month, movement_flag, sell_channel,

-- Customers (serviceno)
count(distinct serviceno) as Sales,
sum(churners_90_1st_bill) as Churners_1st_bill,
sum(rejoiners_1st_bill) as Rejoiners_1st_bill, 
sum(churners_90_2nd_bill) as Churners_2nd_bill,
sum(rejoiners_2nd_bill) as Rejoiners_2nd_bill, 
sum(churners_90_3rd_bill) as Churners_3rd_bill,
sum(rejoiners_3rd_bill) as Rejoiners_3rd_bill, 
sum(voluntary_churners_6_month) as Voluntary_churners, 

-- RGUs (not relevant in mobile)
null as Sales_rgu, 
null as Churners_1st_bill_rgu,
null as Churners_2nd_bill_rgu,
null as Churners_3rd_bill_rgu,
null as rejoiners_1st_bill_rgu,
null as rejoiners_2nd_bill_rgu,
null as rejoiners_3rd_bill_rgu,
null as rejoiners_3rd_bill_rgu

FROM "dg-sandbox"."cwp_sqm_jan22_jun23"
GROUP BY 1,2,3

