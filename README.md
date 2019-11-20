# Opioid Data
The original data set is from the Automated Reports and Consolidated Ordering System (ARCOS) related to opioid sales and distribution. The data was distributed as a 150GB+ TSV file. The goal of the project was to study opioid distribution numbers across the US, but working with the original file was too resource intensive due to its size. I decided to read the data into a PostgreSQL database and then divide the data into separate tables by state, which was accomplished by a combination of SQL and Python code.

Before importing the original file into SQL I performed cleaning steps such as converting the date column to a date format from an int, removing characters from int columns or special characters (/, \\, *, etc.) from other columns.

## Notes about the data
No data was published for the state of Alaska.

The following columns were not included due to having no data, repeated data for every row or data that was not relevant to the project.
REPORTER_DEA_NO,	REPORTER_BUS_ACT, BUYER_DEA_NO, BUYER_ADDL_CO_INFO, TRANSACTION_CODE,	DRUG_CODE,	NDC_NO, ACTION_INDICATOR,	ORDER_FORM_NO,
CORRECTION_NO,	STRENGTH, TRANSACTION_ID,	Product_Name,	Ingredient_Name,	Measure,	MME_Conversion_Factor.
