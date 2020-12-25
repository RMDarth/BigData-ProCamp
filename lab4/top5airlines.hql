!echo Creating database HadoopLab4...;

DROP DATABASE IF EXISTS HadoopLab4 CASCADE;
CREATE DATABASE HadoopLab4;
USE HadoopLab4;

!echo Creating tables...;

-- GLC| The first line in `flights` the is the header, isn't?
CREATE EXTERNAL TABLE IF NOT EXISTS Flights
(YEAR int, MONTH int, DAY int, DAY_OF_WEEK int, 
 AIRLINE string, 
 FLIGHT_NUMBER string, TAIL_NUMBER string, ORIGIN_AIRPORT string, DESTINATION_AIRPORT string, SCHEDULED_DEPARTURE string, DEPARTURE_TIME int, 
 DEPARTURE_DELAY int, 
 TAXI_OUT string, WHEELS_OFF string, SCHEDULED_TIME string, ELAPSED_TIME string, AIR_TIME string, DISTANCE string, 
 WHEELS_ON string, TAXI_IN string, SCHEDULED_ARRIVAL string, ARRIVAL_TIME string, ARRIVAL_DELAY string, DIVERTED string, CANCELLED string, 
 CANCELLATION_REASON string, AIR_SYSTEM_DELAY string, SECURITY_DELAY string, AIRLINE_DELAY string, LATE_AIRCRAFT_DELAY string, WEATHER_DELAY string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///bdpc/hadoop/lab4/flights'
TBLPROPERTIES ("skip.header.line.count"="1");

-- GLC| The first line in `airlines` the is the header, isn't?
CREATE EXTERNAL TABLE IF NOT EXISTS Airlines
(
 AIRLINE string, 
 NAME string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///bdpc/hadoop/lab4/airlines'
TBLPROPERTIES ("skip.header.line.count"="1");

!echo Running query for Top 5 airlines....;

-- GLC| What about table creation?
-- GLC| Is it the best solution in terms of execution performance? What alternatives could you think about?
-- GLC| Recommend: Estimate execution performance with|w/o joining, review execution plan, play with execution engines
SELECT F.AIRLINE, A.NAME, AVG(F.DEPARTURE_DELAY) as AvgDelay 
FROM Flights F JOIN Airlines A 
	ON F.AIRLINE = A.AIRLINE 
GROUP BY F.AIRLINE, A.NAME
ORDER BY AvgDelay DESC LIMIT 5;

-- GLC| Any ideas on general testing solution?
