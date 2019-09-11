register /home/mpcs53013/pig-0.15.0-src/lib/piggybank-0.16.0.jar

-- Load the entire crimes dataset from HDFS
CRIMES_2001_TO_PRESENT = LOAD '/crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
	AS(
	id:int,
	case_number,
	date_time,
	block,
	iucr,
	primary_type,
	description,
	location_description,
	arrest:boolean,
	domestic:boolean,
	beat,
	district,
	ward,
	community_area:int,
	fbi_code,
	x_coordinate:int,
	y_coordinate:int,
	year:int,
	updated_on,
	latitude:double,
	longitude:double,
	location);

ILLUSTRATE CRIMES_2001_TO_PRESENT;

-- Filter out crimes that did not occur between 2008 and 2012
CRIMES_2008_TO_2012 = FILTER CRIMES_2001_TO_PRESENT BY (year >= 2008) AND (year <= 2012);

-- Leave out columns that are not relevant
CRIMES = FOREACH CRIMES_2008_TO_2012 GENERATE 
	id, 
	case_number, 
	date_time, 
	block, 
	-- iucr,	
	primary_type, 
	description, 
	location_description, 
	arrest, 
	domestic, 
	-- beat,
	-- district,
	-- ward,
	community_area, 
	-- fbi_code,
	-- x_coordinate,
	-- y_coordinate,
	year,
	-- updated_on,
	latitude, 
	longitude;
	-- location;
STORE CRIMES INTO '/crimes/' USING PigStorage(',');




	