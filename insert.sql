--truncate opioids;
--select count(*) from opioids

INSERT INTO alabama
SELECT * 
FROM opioids
WHERE buyer_state = ''