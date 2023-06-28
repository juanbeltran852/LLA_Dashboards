-- SELECT * FROM "db-stage-dev"."polaris_campaigns" LIMIT 10 

--- billableaccountno

-- SELECT 
--     distinct billableaccountno, 
--     count(*) num_records
-- FROM "db-stage-dev"."polaris_campaigns" 
-- GROUP BY 1 
-- ORDER BY 
--     billableaccountno 
--         desc 
--         -- asc

--- serviceno

-- SELECT 
--     distinct serviceno, 
--     count(*) num_records
-- FROM "db-stage-dev"."polaris_campaigns" 
-- GROUP BY 1 
-- ORDER BY 
--     serviceno
--         desc 
        -- asc

--- email (registros corridos)

-- SELECT 
--     distinct email, 
--     count(*) num_records
-- FROM "db-stage-dev"."polaris_campaigns" 
-- GROUP BY 1 
-- ORDER BY 
--     email
--         -- desc 
--         asc

--- dias_de_atraso

-- SELECT 
--     distinct dias_de_atraso, 
--     count(*) num_records
-- FROM "db-stage-dev"."polaris_campaigns" 
-- GROUP BY 1 
-- ORDER BY 
--     dias_de_atraso
--         -- desc 
--         asc

--- dt

-- SELECT 
--     distinct dt, 
--     count(*) num_records
-- FROM "db-stage-dev"."polaris_campaigns" 
-- GROUP BY 1 
-- ORDER BY 
--     dt
--         -- desc 
--         asc


--- category

-- SELECT 
--     distinct category, 
--     count(*) num_records
-- FROM "db-stage-dev"."polaris_campaigns" 
-- GROUP BY 1 
-- ORDER BY 
--     category
--         -- desc 
--         asc
        
--- cycle

-- SELECT 
--     distinct cycle, 
--     count(*) num_records
-- FROM "db-stage-dev"."polaris_campaigns" 
-- GROUP BY 1 
-- ORDER BY 
--     cycle
--         -- desc 
--         asc

--- vip

-- SELECT 
--     distinct vip, 
--     count(*) num_records
-- FROM "db-stage-dev"."polaris_campaigns" 
-- GROUP BY 1 
-- ORDER BY 
--     vip
--         -- desc 
--         asc

--- Registros corridos

SELECT
    *
FROM "db-stage-dev"."polaris_campaigns" 
WHERE
    category not in ('"Consumer"', '"Low Risk Consumer"', '"Consumer Mas Control"')
    or vip not in ('"Y"', '"N"')
LIMIT 500
