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
	latitude:double,
	longitude:double);

COMMUNITY_AREA_AND_CRIMES = FOREACH CRIMES GENERATE
	community_area,
	id,
	case_number,
	date_time,
	block,
	primary_type,
	description,
	location_description,
	arrest,
	domestic,
	year,
	latitude,
	longitude;

CRIMES_GROUPED_BY_COMMUNITY = GROUP COMMUNITY_AREA_AND_CRIMES BY community_area;
DESCRIBE CRIMES_GROUPED_BY_COMMUNITY;

CRIMES_BY_COMMUNITY = FOREACH CRIMES_GROUPED_BY_COMMUNITY GENERATE 
	$0 AS community_area,
	$1 AS crime;

STORE CRIMES_BY_COMMUNITY INTO 'hbase://crimes_by_community' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
	'crime:crime');

