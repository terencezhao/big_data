-- Load crimes from /crimes/
CRIMES = LOAD '/crimes/part*' USING PigStorage(',') AS (
	id:int,
	case_number,
	date_time,
	block,
	primary_type,
	description,
	location_description,
	arrest:boolean,
	domestic:boolean,
	community_area:int,
	year:int,
	updated_on,
	latitude:double,
	longitude:double);

-- Load indicators.csv from HDFS. 
INDICATORS = LOAD '/indicators.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
-- This dataset contains socioeconomic hardship indicators for each community area in chicago from 2008-2012
	AS (	
	community_area_number:int, 
	community_area_name, 
	percent_of_housing_crowded:double, 
	percent_households_below_poverty:double, 
	percent_aged_16_plus_unemployed:double, 
	percent_aged_25_plus_without_high_school_diploma:double, 
	percent_aged_under_18_or_over_64:double, 
	per_capita_income:int, 
	hardship_index:int);
  
-- Join crimes with indicators by community area
CRIMES_AND_INDICATORS = JOIN CRIMES by community_area LEFT OUTER, INDICATORS by community_area_number;

-- Store the resulting data into HDFS as a CSV file
STORE CRIMES_AND_INDICATORS INTO '/crimes_and_indicators/' USING PigStorage(',');