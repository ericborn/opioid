SELECT * FROM wyoming
LIMIT 1

-- Counts by drug seller ordered descending
SELECT DISTINCT reporter_address1, COUNT(*) AS count
FROM wyoming
GROUP BY reporter_address1
ORDER BY count DESC

-- Counts by drug buyer ordered descending
SELECT DISTINCT buyer_address1, COUNT(*) AS count
FROM wyoming
GROUP BY buyer_address1
ORDER BY count DESC

-- Counts by buyer 
SELECT DISTINCT buyer_address1, reporter_address1, COUNT(*) AS count
FROM wyoming
GROUP BY buyer_address1, reporter_address1
ORDER BY buyer_address1