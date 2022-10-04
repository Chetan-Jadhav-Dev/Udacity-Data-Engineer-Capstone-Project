# Data Dictionary

## dim_ports

|Field|Type|Description|
|----|-----|-----------|
|port_id|int8|Primary Key|
|port_code|varchar(3) not null|3 character code used for I94 ports|
|port_state|varchar(50)|U.S. state of port|
|average_temperature|numeric(16,3)|Average temperature of port city|

## dim_countries

|Field|Type|Description|
|----|-----|-----------|
|country_id|int8|Primary Key|
|country_code|varchar(3) not null|3 character code used for I94 countries|
|average_temperature|numeric(16,3)|Average temperature of country|

## dim_time

|Field|Type|Description|
|----|-----|-----------|
|sas_timestamp|int4| Primary Key - The SAS timestamp (days since 1/1/1960)|
|year|int4|4 digit year|
|month|int4|Month (1-12)|
|day|int4|Day (1-31)|
|week|int4|Week of Year (1-52)|
|day_of_week|int4|Day of Week (1-7) starting on Sunday|
|quarter|int4|Quarter of Year (1-4)|

## dim_demographics

|Field|Type|Description|
|----|-----|-----------|
|demographics_id|int8|Primary Key|
|port_id|int8|Foreign key to dim_ports|
|median_age|numeric(18,2)|The median age for the demographic|
|male_population|int4|Count of male population for city|
|female_population|int4|Count of female population for city|
|total_population|int8|Count of population for city|
|num_of_veterans|int4|Count of veterans|
|foreign_born|int4|Count of foreign born persons|
|avg_household_size|numeric(18,2)|Average household size in city|
|race|varchar(100)|Race for this demographic|
|demo_count|int4|Count for this demographic|

## dim_airports

|Field|Type|Description|
|----|-----|-----------|
|airport_id|int8|Primary Key|
|port_id|int4|Foreign key to dim_ports|
|airport_type|varchar(256)|Short description of airport type|
|airport_name|varchar(256)|Airport Name|
|elevation_ft|int4|Airport elevation|
|municipality|varchar(256)|Airport municipality|
|gps_code|varchar(256)|Airport GPS code|
|iata_code|varchar(256)|Airport International Air Transport Association code|
|local_code|varchar(256)|Airport local code|
|coordinates|varchar(256)|Airport Coordinates|

## fact_immigration

|Field|Type|Description|
|----|-----|-----------|
|immigration_id|int8|Primary Key|
|country_id|int8|Foreign key to dim_countries|
|port_id|int8|Foreign Key to dim_ports|
|age|int4|Age of immigrant|
|travel_mode|varchar(100)|Mode of travel for immigrant (air, sea, land, etc.)|
|visa_category|varchar(100)|Immigrant VISA category|
|visa_type|varchar(100)|Type of VISA|
|gender|varchar(10)|Immigrant gender|
|birth_year|int4|Immigrant birth year|
|arrdate|int4|SAS timestamp of arrival date, Foreign key to dim_time|
|depdate|int4|SAS timestamp of departure date, Foreign key to dim_time|