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
				SUM(dosage_unit) AS Dosage
FROM wyoming
WHERE dosage_unit IS NOT NULL
GROUP BY reporter_name, year
ORDER BY reporter_name, year, dosage DESC