--DROP TABLE IF EXISTS
--Alabama,Alaska,Arizona,Arkansas,California,Colorado,
--Connecticut,Washington_dc,Delaware,Florida,Georgia,
--Hawaii,Idaho,Illinois,Indiana,Iowa,Kansas,Kentucky,
--Louisiana,Maine,Maryland,Massachusetts,Michigan,
--Minnesota,Mississippi,Missouri,Montana,Nebraska,Nevada,
--New_Hampshire,New_Jersey,New_Mexico,New_York,
--North_Carolina,North_Dakota,Ohio,Oklahoma,Oregon,
--Pennsylvania,Rhode_Island,South_Carolina,South_Dakota,
--Tennessee,Texas,Utah,Vermont,Virginia,Washington,
--West_Virginia,Wisconsin,Wyoming;


CREATE TABLE opioids (
--REPORTER_DEA_NO TEXT,
--REPORTER_BUS_ACT TEXT,
REPORTER_NAME TEXT,
REPORTER_ADDL_CO_INFO TEXT,
REPORTER_ADDRESS1 TEXT,
REPORTER_ADDRESS2 TEXT,
REPORTER_CITY TEXT,
REPORTER_STATE TEXT,
REPORTER_ZIP INTEGER,
REPORTER_COUNTY TEXT,
--BUYER_DEA_NO TEXT,
BUYER_BUS_ACT TEXT,
BUYER_NAME TEXT,
--BUYER_ADDL_CO_INFO TEXT,
BUYER_ADDRESS1 TEXT,
BUYER_ADDRESS2 TEXT,
BUYER_CITY TEXT,
BUYER_STATE TEXT,
BUYER_ZIP INTEGER,
BUYER_COUNTY TEXT,
--TRANSACTION_CODE TEXT,
--DRUG_CODE SMALLINT,
--NDC_NO TEXT,
DRUG_NAME TEXT,
QUANTITY REAL,
UNIT TEXT,
--ACTION_INDICATOR TEXT,
--ORDER_FORM_NO TEXT,
--CORRECTION_NO REAL,
--STRENGTH REAL,
TRANSACTION_DATE DATE, --INTEGER,
CALC_BASE_WT_IN_GM DECIMAL, --REAL,
DOSAGE_UNIT REAL,
--TRANSACTION_ID INTEGER,
Product_Name TEXT,
--Ingredient_Name TEXT,
--Measure TEXT,
--MME_Conversion_Factor SMALLINT, --REAL,
Combined_Labeler_Name TEXT,
Revised_Company_Name TEXT,
Reporter_family TEXT,
dos_str DECIMAL --REAL
);

-- read from csv into table
--COPY opioids FROM 'F:\opioid data\test.csv' DELIMITER ',' CSV HEADER;

--select * from opioids