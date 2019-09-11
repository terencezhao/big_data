register /home/mpcs53013/pig-0.15.0-src/lib/piggybank-0.16.0.jar

BURGLARY = LOAD '/burglary.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
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

BURGLARY = FILTER BURGLARY BY (year >= 2015) AND (year <= 2016);

BURGLARY = FOREACH BURGLARY GENERATE 
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

DUMP BURGLARY;