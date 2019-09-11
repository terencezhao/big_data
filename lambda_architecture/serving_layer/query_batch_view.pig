CRIMES = LOAD '/crimes_and_indicators' USING PigStorage(',') AS (
	id:int
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
	longitude:double
);

CRIMES_BY_COMMUNITY = GROUP CRIMES BY community_area;
COMMUNITY_TO_NUMBER_OF_CRIMES = FOREACH CRIMES_BY_COMMUNITY GENERATE 
	community_area

STORE CRIMES_BY_COMMUNITY INTO 'hbase://crimes_by_community' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(

