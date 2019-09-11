#!/usr/bin/perl -w
# Creates an html table of flight delays by weather for the given route

# Needed includes
use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;

# Read the origin and destination airports as CGI parameters
my $community = param('community');

 
# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server. The first version
# is for our VM, the second is for running on the class cluster
my $hbase = HBase::JSONRest->new(host => "localhost:8080");
# my $hbase = HBase::JSONRest->new(host => "http://hdp-m.c.mpcs53013-2016.internal:2056/");

# This function takes a row and gives you the value of the given column
# E.g., cellValue($row, 'delay:rain_delay') gives the value of the
# rain_delay column in the delay family.
# It uses somewhat tricky perl, so you can treat it as a black box
sub cellValue {
    my $row = $_[0];
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
	if ($$cell{'name'} eq $field_name) {
	    return $$cell{'value'};
	}
    }
    return 'missing';
}

# Query hbase for the route. For example, if the departure airport is ORD
# and the arrival airport is DEN, the "where" clause of the query will
# require the key to equal ORDDEN
my $records = $hbase->get({
  table => 'crime_type_by_community',
  where => {
    key_equals => $community
  },
});

# There will only be one record for this route, which will be the
# "zeroth" row returned
my $row = @$records[0];

# Get the value of all the columns we need and store them in named variables
# Perl's ability to assign a list of values all at once is very convenient here
my(
	$arson, 
	$assault, 
	$battery, 
	$burglary, 
	$concealed_carry_licence_violation, 
	$crim_sexual_assault,
   	$criminal_damage, 
	$damage, 
	$criminal_trespass, 
	$deceptive_practice, 
	$domestic_violence, 
	$gambling,
   	$homocide, 
	$human_trafficking, 
	$interference_with_public_officer, 
	$intimidation, 
	$kidnapping, 
	$liquor_law_violation,
	$motor_vehicle_theft,
	$narcotics,
	$non_criminal,
	$non_criminal_subject_specified,
	$obscenity,
	$offense_involving_children,
	$other_narcotic_violation,
	$other_offense,
	$prostitution,
	$public_indecency,
	$public_peace_violation,
	$ritualism,
	$robbery,
	$sex_offense,
	$stalking,
	$theft,
	$weapons_violation)
 =  (
	cellValue($row, 'type:arson'), 
	cellValue($row, 'type:assault'),
     	cellValue($row, 'type:battery'), 
	cellValue($row, 'delay:burglary'),
     	cellValue($row, 'type:concealed_carry_licence_violation'), 
	cellValue($row, 'type:crim_sexual_assault'),
	cellValue($row, 'type:criminal_damage'), 
	cellValue($row, 'type:damage'),
	cellValue($row, 'type:criminal_trespass'),
	cellValue($row, 'type:deceptive_practice'), 
	cellValue($row, 'type:domestic_violence'),
	cellValue($row, 'type:gambling'), 
	cellValue($row, 'type:homocide'),
	cellValue($row, 'type:human_trafficking'), 
	cellValue($row, 'type:interference_with_public_officer'),
	cellValue($row, 'type:intimidation'), 
	cellValue($row, 'type:kidnapping'),
	cellValue($row, 'type:liquor_law_violation'), 
	cellValue($row, 'type:motor_vehicle_theft'),
	cellValue($row, 'type:narcotics'), 
	cellValue($row, 'type:non_criminal'),
	cellValue($row, 'type:non_criminal_subject_specified'), 
	cellValue($row, 'type:obscenity'),
	cellValue($row, 'type:offense_involving_children'), 
	cellValue($row, 'type:other_narcotic_violation'),
	cellValue($row, 'type:other_offense'), 
	cellValue($row, 'type:prostitution'),
	cellValue($row, 'type:public_indecency'), 
	cellValue($row, 'type:public_peace_violation'),
	cellValue($row, 'type:ritualism'), 
	cellValue($row, 'type:robbery'),
	cellValue($row, 'type:sex_offense'), 
	cellValue($row, 'type:stalking'),
	cellValue($row, 'type:theft'), 
	cellValue($row, 'type:weapons_violation'));

my @types = (
	{ type => "ARSON", count => $arson }, 
	{ type => "ASSAULT", count => $assault },
	{ type => "BATTERY", count => $battery },
	{ type => "BURGLARY", count => $burglary }, 
	{ type => "CONCEALED CARRY LICENCE VIOLATION", count => $concealed_carry_licence_violation }, 
	{ type => "CRIM SEXUAL ASSAULT", count => $crim_sexual_assault }, 
	{ type => "CRIMINAL DAMAGE", count => $criminal_damage }, 
	{ type => "DAMAGE", count => $damage }, 
	{ type => "CRIMINAL TRESPASS", count => $criminal_trespass }, 
	{ type => "DECEPTIVE PRACTICE", count => $deceptive_practice }, 
	{ type => "DOMESTIC VIOLENCE", count => $domestic_violence }, 
	{ type => "GAMBLING", count => $gambling }, 
	{ type => "HOMOCIDE", count => $homocide }, 
	{ type => "HUMAN TRAFFICKING", count => $human_trafficking }, 
	{ type => "INTERFERENCE wITH PUBLIC OFFICER", count => $interference_with_public_officer }, 
	{ type => "INTIMIDATION", count => $intimidation }, 
	{ type => "KIDNAPPING", count => $kidnapping }, 
	{ type => "LIQUOR_LAW_VIOLATION", count => $liquor_law_violation }, 
	{ type => "MOTOR VEHICLE THEFT", count => $motor_vehicle_theft }, 
	{ type => "NARCOTICS", count => $narcotics }, 
	{ type => "NON-CRIMINAL", count => $non_criminal }, 
	{ type => "NON-CRIMINAL (SUBJECT SPECIFIED)", count => $non_criminal_subject_specified }, 
	{ type => "OBSCENITY", count => $obscenity }, 
	{ type => "OFFENSE INVOLVING CHILDREN", count => $offense_involving_children }, 
	{ type => "OTHER NARCOTIC VIOLATION", count => $other_narcotic_violation },
	{ type => "OTHER OFFENSE", count => $other_offense },
	{ type => "PROSTITUTION", count => $prostitution },
	{ type => "PUBLIC INDECENCY", count => $public_indecency },
	{ type => "PUBLIC PEACE VIOLATION", count => $public_peace_violation },
	{ type => "RITUALISM", count => $ritualism },
	{ type => "ROBBERY", count => $robbery },
	{ type => "SEX OFFENSE", count => $sex_offense },
	{ type => "STALKING", count => $stalking },
	{ type => "THEFT", count => $theft },
	{ type => "WEAPONS VIOLATION", count => $weapons_violation });
 
 

my @descending = sort { $b->{count} <=> $a->{count} } @types;

# Print an HTML page with the table. Perl CGI has commands for all the
# common HTML tags
print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));

print div({-style=>'margin-left:275px;margin-right:auto;display:inline-block;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Crimes by Type from community: ' . $community . ' &nbsp;');
print     p({-style=>"bottom-margin:10px"});
print table({-class=>'CSS_Table_Example', -style=>'width:60%;margin:auto;'},
	    Tr([td(['FIRST', 'SECOND', 'THIRD', 'FOURTH', 'FIFTH']),
                td([$descending[0]->{type}, $descending[1]->{type}, $descending[2]->{type}, $descending[3]->{type}, $descending[4]->{type}])
		])),
    p({-style=>"bottom-margin:10px"})
    ;

print end_html;
