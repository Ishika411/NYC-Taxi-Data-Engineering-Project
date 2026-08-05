CREATE DATABASE gold;

SELECT * FROM gold.trip_data;

SELECT * FROM gold.trip_zone
WHERE Borough = 'EWR';

UPDATE gold.trip_zone
SET Borough = 'EMR'
WHERE Borough = 'EWR';

DELETE FROM gold.trip_zone
WHERE Borough = 'EMR';

DESCRIBE HISTORY gold.trip_zone;

SELECT * FROM gold.trip_zone
VERSION AS OF 0
WHERE Borough = 'EWR';

RESTORE TABLE gold.trip_zone
TO VERSION AS OF 0;

VACUUM gold.trip_zone;
