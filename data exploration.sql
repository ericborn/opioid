/*
!!!TODO!!!
Fix blank dosage_units
SELECT * 
FROM wyoming
WHERE dosage_unit IS NULL
LIMIT 10

*/
SELECT * 
FROM wyoming
--WHERE dosage_unit IS NULL
LIMIT 10

-- Transaction counts by drug seller ordered descending
SELECT DISTINCT reporter_address1, COUNT(*) AS count
FROM wyoming
GROUP BY reporter_address1
ORDER BY count DESC

-- Transaction counts by drug buyer ordered descending
SELECT DISTINCT buyer_address1, COUNT(*) AS count
FROM wyoming
GROUP BY buyer_address1
ORDER BY count DESC

-- Transaction counts grouped by buyer and seller
SELECT DISTINCT buyer_address1, reporter_address1, COUNT(*) AS count
FROM wyoming
GROUP BY buyer_address1, reporter_address1
ORDER BY buyer_address1, count DESC

-- Dosage sum by drug seller grouped by reporter_name and year
SELECT DISTINCT reporter_name, EXTRACT(YEAR FROM transaction_date) AS year,
				ROUND(SUM(dosage_unit)) AS Dosage
FROM wyoming
WHERE dosage_unit IS NOT NULL
GROUP BY reporter_name, year
ORDER BY reporter_name, year, dosage DESC

-- Dosage sum by buyer_city grouped by buyer_city and year
SELECT DISTINCT buyer_city, EXTRACT(YEAR FROM transaction_date) AS year,
				ROUND(SUM(dosage_unit)) AS Dosage
FROM wyoming
WHERE dosage_unit IS NOT NULL
GROUP BY buyer_city, year
ORDER BY buyer_city, year, dosage DESC

-- TODO
-- Numbers need to be evaluated for accuracy
-- Join zip on state table to find population and total dosage by zip
SELECT DISTINCT buyer_zip, EXTRACT(YEAR FROM transaction_date) AS year, z.population,
				ROUND(SUM(dosage_unit)) AS Dosage, 
				(ROUND(SUM(dosage_unit)) / z.population) AS ratio
FROM wyoming w
INNER JOIN zip z ON (z.zip = w.buyer_zip)
WHERE dosage_unit IS NOT NULL AND EXTRACT(YEAR FROM transaction_date) = 2010
GROUP BY buyer_zip, year, population
ORDER BY dosage DESC