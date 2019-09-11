DROP TABLE IF EXISTS ChicagoCrime;
CREATE EXTERNAL TABLE IF NOT EXISTS ChicagoCrime (
	id int, 
	case_number string, 
	date_time string, 
	block string, 
	primary_type string, 
	description string, 
	location_description string, 
	arrest boolean, 
	domestic boolean, 
	community_area int, 
	year int, 
	latitude double, 
	longitude double
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/crimes.csv' OVERWRITE INTO TABLE ChicagoCrime;

SELECT * from ChicagoCrime LIMIT 10;

