SELECT * FROM earthquakes.usgs_earthquake;

-- Import Big Dataset(~170,000 records)
CREATE TABLE `usgs_earthquake` (
  `time` VARCHAR(255),
  `latitude` double ,
  `longitude` double ,
  `depth` double ,
  `mag` double ,
  `magType` VARCHAR(255),
  `nst` int ,
  `gap` double ,
  `dmin` double ,
  `rms` double,
  `net` VARCHAR(255),
  `id` VARCHAR(255),
  `updated` DATETIME(3),
  `place` VARCHAR(255),
  `type` VARCHAR(255),
  `horizontalError` double ,
  `depthError` double ,
  `magError` double ,
  `magNst` int ,
  `status` VARCHAR(255),
  `locationSource` VARCHAR(255),
  `magSource` VARCHAR(255)
);

LOAD DATA INFILE 'usgs_earthquake.csv'
INTO TABLE usgs_earthquake
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES;

-- Set Datetime Format
ALTER TABLE usgs_earthquake MODIFY COLUMN `time` DATETIME;
UPDATE usgs_earthquake SET `time` = STR_TO_DATE(`time`, '%m/%d/%Y %H:%i');


-- Overview of Dataset
SELECT * FROM earthquakes.usgs_earthquake;
-- LIMIT 1000000;


-- Top 20 Strongest Earthquakes Events
SELECT `time`, place, mag 
FROM usgs_earthquake
ORDER BY mag DESC
LIMIT 20;

-- Earthquakes in Japan
SELECT `time`, place, mag 
FROM usgs_earthquake
WHERE place LIKE '%Japan%';

-- Count All Earthquakes
SELECT count(*)
FROM usgs_earthquake;


-- Average Magnitude and Depth per Magnitude Type
SELECT magType, AVG(mag), AVG(depth), COUNT(magType)
FROM usgs_earthquake
GROUP BY magType;


-- Earthquakes per Year
WITH cnt AS
(
SELECT year(`time`) `year`
FROM usgs_earthquake
)
SELECT `year`, COUNT(`year`) AS Event_Count
FROM cnt
GROUP BY `year`
ORDER BY `year`;


-- Strong Earthquakes in Indonesia
SELECT `time`, place, mag ,depth
FROM usgs_earthquake
WHERE place LIKE '%Indonesia%'
AND mag >=7;

-- Top 20 Most Active Locations
SELECT place, COUNT(place) count_place
FROM usgs_earthquake
GROUP BY place
ORDER BY count_place DESC
LIMIT 20;


-- Strongest Earthquake Each Year
WITH yr_table AS
(
SELECT 
year(`time`) `year`,`time`, place, mag
FROM usgs_earthquake
),
max_table AS 
(
SELECT 
`year`,
MAX(mag) MAX_mag,
count(*) Count_year
FROM yr_table
GROUP BY `year`
ORDER BY `year`
)
SELECT *
FROM yr_table t1
JOIN max_table t2 
ON t1.mag = t2.MAX_mag AND t1.`year` = t2.`year`
ORDER BY t2.`year`
;


-- Earthquakes with High Magnitude Uncertainty
WITH mag AS
(
SELECT magSource, MAX(magError) maxmag_error
FROM usgs_earthquake
WHERE magError IS NOT NULL
GROUP BY magSource
)
SELECT t1.magSource,
t2.`time`,
t1.maxmag_error
FROM mag t1
JOIN usgs_earthquake t2
ON t1.maxmag_error = t2.magError AND t1.magSource = t2.magSource
;


-- Running Average of Magnitude
SELECT `time`, magType, mag,
AVG(mag) OVER(PARTITION BY magType ORDER BY `time`) running_avg
FROM usgs_earthquake;


-- Earthquakes within 24 hours interval of the 2011 Tohoku Event
SELECT *
FROM usgs_earthquake
WHERE place = '2011 Great Tohoku Earthquake, Japan' AND `time` = '2011-03-11 05:46';

SELECT *
FROM usgs_earthquake
WHERE `time`
BETWEEN DATE_SUB('2011-03-11 05:46', INTERVAL 24 HOUR) 
AND DATE_ADD('2011-03-11 05:46', INTERVAL 24 HOUR);
