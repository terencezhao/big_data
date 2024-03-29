CRIMES = LOAD '/burglary/part*' USING PigStorage(',') AS (
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
	(primary_type == 'ARSON' ? 1L : 0L) AS arson,
	(primary_type == 'ASSAULT' ? 1L : 0L) AS assault,
	(primary_type == 'BATTERY' ? 1L : 0L) AS battery,
	(primary_type == 'BURGLARY' ? 1L : 0L) AS burglary,
	(primary_type == 'CONCEALED CARRY LICENCE VIOLATION' ? 1L : 0L) AS concealed_carry_licence_violation,
	(primary_type == 'CRIM SEXUAL ASSAULT' ? 1L : 0L) AS crim_sexual_assault,
	(primary_type == 'CRIMINAL DAMAGE' ? 1L : 0L) AS criminal_damage,
	(primary_type == 'DAMAGE' ? 1L : 0L) AS damage,
	(primary_type == 'CRIMINAL TRESPASS' ? 1L : 0L) AS criminal_trespass,
	(primary_type == 'DECEPTIVE PRACTICE' ? 1L : 0L) AS deceptive_practice,
	(primary_type == 'DOMESTIC VIOLENCE' ? 1L : 0L) AS domestic_violence,
	(primary_type == 'GAMBLING' ? 1L : 0L) AS gambling,
	(primary_type == 'HOMOCIDE' ? 1L : 0L) AS homocide,
	(primary_type == 'HUMAN TRAFFICKING' ? 1L : 0L) AS human_trafficking,
	(primary_type == 'INTERFERENCE wITH PUBLIC OFFICER' ? 1L : 0L) AS interference_with_public_officer,
	(primary_type == 'INTIMIDATION' ? 1L : 0L) AS intimidation,
	(primary_type == 'KIDNAPPING' ? 1L : 0L) AS kidnapping,
	(primary_type == 'LIQUOR_LAW_VIOLATION' ? 1L : 0L) AS liquor_law_violation,
	(primary_type == 'MOTOR VEHICLE THEFT' ? 1L : 0L) AS motor_vehicle_theft,
	(primary_type == 'NARCOTICS' ? 1L : 0L) AS narcotics,
	(primary_type == 'NON-CRIMINAL' ? 1L : 0L) AS non_criminal,
	(primary_type == 'NON-CRIMINAL (SUBJECT SPECIFIED)' ? 1L : 0L) AS non_criminal_subject_specified,
	(primary_type == 'OBSCENITY' ? 1L : 0L) AS obscenity,
	(primary_type == 'OFFENSE INVOLVING CHILDREN' ? 1L : 0L) AS offense_involving_children,
	(primary_type == 'OTHER NARCOTIC VIOLATION' ? 1L : 0L) AS other_narcotic_violation,
	(primary_type == 'OTHER OFFENSE' ? 1L : 0L) AS other_offense,
	(primary_type == 'PROSTITUTION' ? 1L : 0L) AS prostitution,
	(primary_type == 'PUBLIC INDECENCY' ? 1L : 0L) AS public_indecency,
	(primary_type == 'PUBLIC PEACE VIOLATION' ? 1L : 0L) AS public_peace_violation,
	(primary_type == 'RITUALISM' ? 1L : 0L) AS ritualism,
	(primary_type == 'ROBBERY' ? 1L : 0L) AS robbery,
	(primary_type == 'SEX OFFENSE' ? 1L : 0L) AS sex_offense,
	(primary_type == 'STALKING' ? 1L : 0L) AS stalking,
	(primary_type == 'THEFT' ? 1L : 0L) AS theft,
	(primary_type == 'WEAPONS VIOLATION' ? 1L : 0L) AS weapons_violation;

CRIMES_GROUPED_BY_COMMUNITY = GROUP COMMUNITY_AREA_AND_CRIMES BY community_area;
DESCRIBE CRIMES_GROUPED_BY_COMMUNITY;

-- ARSON, ASSAULT, BATTERY, BURGLARY, CONCEALED CARRY LICENCE VIOLATION, CRIM SEXUAL ASSAULT, 
-- CRIMINAL DAMAGE, CRIMINAL TRESPASS, DECEPTIVE PRACTICE, DOMESTIC VIOLENCE, GAMBLING, HOMOCIDE,
-- HUMAN TRAFFICKING, INTERFERENCE WITH PUBLIC OFFICER, INTIMIDATION, KIDNAPPING, LIQUOR LAW VIOLATION, 
-- MOTOR VEHICLE THEFT, NARCOTICS, NON-CRIMINAL, NON-CRIMINAL(SUBJECT SPECIFIED), OBSCENITY,
-- OFFENSE INVOLVING CHILDREN, OTHER NARCOTIC VIOLATION, OTHER OFFENSE, PROSTITUTION, PUBLIC INDECENCY,
-- PUBLIC PEACE VIOLATION, RITUALISM, ROBBERY, SEX OFFENSE, STALKING, THEFT, WEAPONS VIOLATION
CRIME_TYPE_BY_COMMUNITY = FOREACH CRIMES_GROUPED_BY_COMMUNITY GENERATE 
	$0 AS community_area,
	SUM($1.arson) AS arson, 
	SUM($1.assault) AS assault, 
	SUM($1.battery) AS battery,
	SUM($1.burglary) AS burglary,
	SUM($1.concealed_carry_licence_violation) AS concealed_carry_licence_violation,
	SUM($1.crim_sexual_assault) AS crim_sexual_assault,
	SUM($1.criminal_damage) AS criminal_damage,
	SUM($1.criminal_trespass) AS criminal_trespass,
	SUM($1.deceptive_practice) AS deceptive_practice,
	SUM($1.domestic_violence) AS domestic_violence,
	SUM($1.gambling) AS gambling,
	SUM($1.homocide) AS homocide,
	SUM($1.human_trafficking) AS human_trafficking, 
	SUM($1.interference_with_public_officer) AS interference_with_public_officer, 
	SUM($1.intimidation) AS intimidation, 
	SUM($1.kidnapping) AS kidnapping, 
	SUM($1.liquor_law_violation) AS liquor_law_violation, 
	SUM($1.motor_vehicle_theft) AS motor_vehicle_theft, 
	SUM($1.narcotics) AS narcotics, 
	SUM($1.non_criminal) AS non_criminal, 
	SUM($1.non_criminal_subject_specified) AS non_criminal_subject_specified, 
	SUM($1.obscenity) AS obscenity, 
	SUM($1.offense_involving_children) AS offense_involving_children, 
	SUM($1.other_narcotic_violation) AS other_narcotic_violation,
	SUM($1.other_offense) AS other_offense,  
	SUM($1.prostitution) AS prostitution,  
	SUM($1.public_indecency) AS public_indecency,  
	SUM($1.public_peace_violation) AS public_peace_violation,  
	SUM($1.ritualism) AS ritualism,  
	SUM($1.robbery) AS robbery,
	SUM($1.sex_offense) AS sex_offense,
	SUM($1.stalking) AS stalking,
	SUM($1.theft) AS theft,
	SUM($1.weapons_violation) AS weapons_violation;

STORE CRIME_TYPE_BY_COMMUNITY INTO 'hbase://crime_type_by_community' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
	'type:arson, type:assault, type:battery, type:burglary, type:concealed_carry_licence_violation, type:crim_sexual_assault, type:criminal_damage, type:criminal_trespass, type:deceptive_practice, type:domestic_violence, type:gambling, type:homocide, type:homocide, type:human_trafficking, type:interference_with_public_officer, type:intimidation, type:kidnapping, type:liquor_law_violation, type:motor_vehicle_theft, type:narcotics, type:non_criminal, type:non_criminal_subject_specified, type:obscenity, type:offense_involving_children, type:other_narcotic_violation, type:other_offense, type:prostitution, type:public_indecency, type:public_peace_violation, type:ritualism, type:robbery, type:sex_offense, type:stalking, type:theft, type:weapons_violation');

