-- ================================================
-- NYC Taxi Trip Prediction - FULL EXTENSIVE PIPELINE
-- Author: ChatGPT
-- DBMS: PostgreSQL
-- ================================================

-- Assumptions:
-- Raw data in raw_taxi_trips table with columns:
-- pickup_datetime TIMESTAMP,
-- dropoff_datetime TIMESTAMP,
-- passenger_count INT,
-- pickup_longitude FLOAT,
-- pickup_latitude FLOAT,
-- dropoff_longitude FLOAT,
-- dropoff_latitude FLOAT,
-- trip_distance FLOAT (miles),
-- fare_amount FLOAT,
-- payment_type VARCHAR (optional),
-- store_and_fwd_flag CHAR (optional)

-- ------------------------------------------------
-- STEP 1: DATA QUALITY CHECKS & CLEANING
-- ------------------------------------------------

-- 1.1 Basic data sanity check: count, nulls, ranges
CREATE OR REPLACE VIEW v_data_quality_check AS
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE pickup_datetime IS NULL) AS null_pickup_datetime,
    COUNT(*) FILTER (WHERE dropoff_datetime IS NULL) AS null_dropoff_datetime,
    COUNT(*) FILTER (WHERE passenger_count <= 0 OR passenger_count > 6) AS invalid_passenger_count,
    COUNT(*) FILTER (WHERE pickup_latitude NOT BETWEEN 40.5 AND 41.0) AS invalid_pickup_latitude,
    COUNT(*) FILTER (WHERE dropoff_latitude NOT BETWEEN 40.5 AND 41.0) AS invalid_dropoff_latitude,
    COUNT(*) FILTER (WHERE pickup_longitude NOT BETWEEN -74.5 AND -73.5) AS invalid_pickup_longitude,
    COUNT(*) FILTER (WHERE dropoff_longitude NOT BETWEEN -74.5 AND -73.5) AS invalid_dropoff_longitude,
    COUNT(*) FILTER (WHERE dropoff_datetime <= pickup_datetime) AS invalid_datetime_order,
    COUNT(*) FILTER (WHERE trip_distance <= 0 OR trip_distance > 200) AS invalid_trip_distance
FROM raw_taxi_trips;


-- 1.2 Create clean_taxi_trips table after removing invalid records
DROP TABLE IF EXISTS clean_taxi_trips CASCADE;
CREATE TABLE clean_taxi_trips AS
SELECT
    *,
    EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) AS trip_duration_sec
FROM raw_taxi_trips
WHERE
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime > pickup_datetime
    AND passenger_count BETWEEN 1 AND 6
    AND pickup_latitude BETWEEN 40.5 AND 41.0
    AND dropoff_latitude BETWEEN 40.5 AND 41.0
    AND pickup_longitude BETWEEN -74.5 AND -73.5
    AND dropoff_longitude BETWEEN -74.5 AND -73.5
    AND trip_distance > 0
    AND trip_distance <= 200 -- Filter very long trips
    AND EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) BETWEEN 60 AND 7200 -- 1 min to 2 hours
;

-- ------------------------------------------------
-- STEP 2: FEATURE ENGINEERING - TEMPORAL, SPATIAL, TRIP FEATURES
-- ------------------------------------------------

DROP TABLE IF EXISTS taxi_features CASCADE;
CREATE TABLE taxi_features AS
SELECT
    *,
    
    -- DATETIME FEATURES
    EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM pickup_datetime) AS pickup_month,
    EXTRACT(DAY FROM pickup_datetime) AS pickup_day,
    EXTRACT(DOW FROM pickup_datetime) AS pickup_dayofweek, -- Sunday=0
    EXTRACT(ISODOW FROM pickup_datetime) AS pickup_isodow, -- Monday=1
    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
    EXTRACT(MINUTE FROM pickup_datetime) AS pickup_minute,
    
    -- FLAG WEEKEND
    CASE WHEN EXTRACT(DOW FROM pickup_datetime) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    
    -- FLAG HOLIDAY (simple static dates, can be expanded)
    CASE
      WHEN (EXTRACT(MONTH FROM pickup_datetime) = 1 AND EXTRACT(DAY FROM pickup_datetime) = 1) THEN TRUE -- New Year
      WHEN (EXTRACT(MONTH FROM pickup_datetime) = 7 AND EXTRACT(DAY FROM pickup_datetime) = 4) THEN TRUE -- Independence Day
      WHEN (EXTRACT(MONTH FROM pickup_datetime) = 12 AND EXTRACT(DAY FROM pickup_datetime) = 25) THEN TRUE -- Christmas
      ELSE FALSE
    END AS is_holiday,
    
    -- FLAG RUSH HOUR (7-9 AM and 4-7 PM on weekdays)
    CASE
      WHEN EXTRACT(DOW FROM pickup_datetime) BETWEEN 1 AND 5
           AND (pickup_hour BETWEEN 7 AND 9 OR pickup_hour BETWEEN 16 AND 19) THEN TRUE
      ELSE FALSE
    END AS is_rush_hour,
    
    -- TRIP DURATION ALREADY COMPUTED as trip_duration_sec
    
    -- HAVERSINE DISTANCE (km)
    2 * 6371 * ASIN(SQRT(
        POWER(SIN(RADIANS((dropoff_latitude - pickup_latitude)/2)), 2) +
        COS(RADIANS(pickup_latitude)) * COS(RADIANS(dropoff_latitude)) *
        POWER(SIN(RADIANS((dropoff_longitude - pickup_longitude)/2)), 2)
    )) AS haversine_distance_km,
    
    -- MANHATTAN DISTANCE (approximate in km)
    (
        (ABS(pickup_latitude - dropoff_latitude) * 111) +
        (ABS(pickup_longitude - dropoff_longitude) * 85)
    ) AS manhattan_distance_km,
    
    -- AVERAGE SPEED (km/h)
    CASE
      WHEN EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) > 0 THEN
          (2 * 6371 * ASIN(SQRT(
              POWER(SIN(RADIANS((dropoff_latitude - pickup_latitude)/2)), 2) +
              COS(RADIANS(pickup_latitude)) * COS(RADIANS(dropoff_latitude)) *
              POWER(SIN(RADIANS((dropoff_longitude - pickup_longitude)/2)), 2)
          ))) / (EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 3600)
      ELSE NULL
    END AS avg_speed_kmh,
    
    -- PASSENGER COUNT GROUP
    CASE 
      WHEN passenger_count = 1 THEN 'single'
      WHEN passenger_count BETWEEN 2 AND 4 THEN 'small_group'
      ELSE 'large_group'
    END AS passenger_group,
    
    -- STORE AND FORWARD FLAG TO BOOLEAN
    CASE WHEN store_and_fwd_flag = 'Y' THEN TRUE ELSE FALSE END AS store_and_forward,

    -- PAYMENT TYPE CATEGORY (if available)
    payment_type
    
FROM clean_taxi_trips
;

-- ------------------------------------------------
-- STEP 3: REMOVE OUTLIERS BASED ON FEATURE ENGINEERING
-- ------------------------------------------------

DELETE FROM taxi_features
WHERE
    trip_duration_sec > 7200 -- > 2 hours
    OR haversine_distance_km > 100 -- > 100 km
    OR avg_speed_kmh > 120 -- > 120 km/h (unrealistic)
    OR avg_speed_kmh < 1 -- < 1 km/h (unrealistic slow)
;

-- ------------------------------------------------
-- STEP 4: CREATE TRAIN / TEST / VALIDATION SPLITS
-- ------------------------------------------------

-- Example splits based on pickup_datetime
-- Train: before 2016-06-01
-- Validation: 2016-06-01 to 2016-07-31
-- Test: after 2016-07-31

DROP TABLE IF EXISTS taxi_train CASCADE;
CREATE TABLE taxi_train AS
SELECT * FROM taxi_features WHERE pickup_datetime < '2016-06-01';

DROP TABLE IF EXISTS taxi_val CASCADE;
CREATE TABLE taxi_val AS
SELECT * FROM taxi_features 
WHERE pickup_datetime >= '2016-06-01' AND pickup_datetime < '2016-08-01';

DROP TABLE IF EXISTS taxi_test CASCADE;
CREATE TABLE taxi_test AS
SELECT * FROM taxi_features WHERE pickup_datetime >= '2016-08-01';

-- ------------------------------------------------
-- STEP 5: FEATURE AGGREGATION & WINDOW FUNCTIONS FOR CONTEXTUAL FEATURES
-- ------------------------------------------------

-- 5.1 Rolling average trip duration per pickup_hour over last 7 days
DROP TABLE IF EXISTS rolling_avg_duration_hour CASCADE;
CREATE TABLE rolling_avg_duration_hour AS
SELECT DISTINCT pickup_hour
FROM taxi_features
ORDER BY pickup_hour; -- placeholder table to hold hours (could be expanded)

-- Example of rolling window using window functions (PostgreSQL specific)
-- Note: This assumes daily aggregation exists; to illustrate, we create daily aggregates first:

DROP TABLE IF EXISTS daily_avg_duration CASCADE;
CREATE TABLE daily_avg_duration AS
SELECT
    DATE(pickup_datetime) AS pickup_date,
    pickup_hour,
    AVG(trip_duration_sec) AS avg_trip_duration_sec,
    COUNT(*) AS trip_count
FROM taxi_features
GROUP BY pickup_date, pickup_hour
ORDER BY pickup_date, pickup_hour;

-- Rolling 7-day average by pickup_hour:
DROP TABLE IF EXISTS rolling_7d_avg_duration_hour CASCADE;
CREATE TABLE rolling_7d_avg_duration_hour AS
SELECT
    pickup_hour,
    pickup_date,
    AVG(avg_trip_duration_sec) OVER (
      PARTITION BY pickup_hour
      ORDER BY pickup_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_trip_duration_sec
FROM daily_avg_duration
ORDER BY pickup_hour, pickup_date;

-- ------------------------------------------------
-- STEP 6: CREATE LAG FEATURES TO CAPTURE TEMPORAL DEPENDENCIES
-- ------------------------------------------------

-- Lag features: average trip duration previous day for same pickup hour

DROP TABLE IF EXISTS taxi_features_with_lags CASCADE;
CREATE TABLE taxi_features_with_lags AS
SELECT
    tf.*,
    lag(daily_avg.avg_trip_duration_sec, 1) OVER (
      PARTITION BY tf.pickup_hour
      ORDER BY DATE(tf.pickup_datetime)
    ) AS prev_day_avg_duration_hour
FROM taxi_features tf
LEFT JOIN daily_avg_duration daily_avg
  ON DATE(tf.pickup_datetime) = daily_avg.pickup_date
  AND tf.pickup_hour = daily_avg.pickup_hour
;

-- ------------------------------------------------
-- STEP 7: SPATIAL BUCKETING (GRID-BASED CLUSTERS)
-- ------------------------------------------------

-- Create simple spatial buckets by rounding lat/lon to grids ~ 500m x 500m (~0.005 degrees)
ALTER TABLE taxi_features_with_lags
ADD COLUMN pickup_lat_bucket INT,
ADD COLUMN pickup_lon_bucket INT,
ADD COLUMN dropoff_lat_bucket INT,
ADD COLUMN dropoff_lon_bucket INT;

UPDATE taxi_features_with_lags SET
    pickup_lat_bucket = FLOOR(pickup_latitude / 0.005),
    pickup_lon_bucket = FLOOR(pickup_longitude / 0.005),
    dropoff_lat_bucket = FLOOR(dropoff_latitude / 0.005),
    dropoff_lon_bucket = FLOOR(dropoff_longitude / 0.005);

-- ------------------------------------------------
-- STEP 8: AGGREGATE AVERAGES BY SPATIAL BUCKETS
-- ------------------------------------------------

DROP TABLE IF EXISTS spatial_bucket_averages CASCADE;
CREATE TABLE spatial_bucket_averages AS
SELECT
    pickup_lat_bucket,
    pickup_lon_bucket,
    AVG(trip_duration_sec) AS avg_trip_duration_sec,
    AVG(haversine_distance_km) AS avg_distance_km,
    COUNT(*) AS trip_count
FROM taxi_features_with_lags
GROUP BY pickup_lat_bucket, pickup_lon_bucket;

-- ------------------------------------------------
-- STEP 9: FINAL DATA PREPARATION FOR ML EXPORT
-- ------------------------------------------------

DROP TABLE IF EXISTS final_taxi_features CASCADE;
CREATE TABLE final_taxi_features AS
SELECT
    pickup_datetime,
    dropoff_datetime,
    trip_duration_sec,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_dayofweek,
    pickup_hour,
    is_weekend,
    is_holiday,
    is_rush_hour,
    passenger_count,
    passenger_group,
    store_and_forward,
    payment_type,
    haversine_distance_km,
    manhattan_distance_km,
    avg_speed_kmh,
    prev_day_avg_duration_hour,
    pickup_lat_bucket,
    pickup_lon_bucket,
    dropoff_lat_bucket,
    dropoff_lon_bucket
FROM taxi_features_with_lags
;

-- ------------------------------------------------
-- STEP 10: SUMMARY STATISTICS FOR EXPLORATORY ANALYSIS
-- ------------------------------------------------

-- Avg fare by hour and passenger group
CREATE OR REPLACE VIEW v_avg_fare_by_hour_passenger AS
SELECT
    pickup_hour,
    passenger_group,
    AVG(fare_amount) AS avg_fare,
    COUNT(*) AS trips
FROM final_taxi_features
GROUP BY pickup_hour, passenger_group
ORDER BY pickup_hour, passenger_group;

-- Trips per day of week and weekend flag
CREATE OR REPLACE VIEW v_trip_counts_dow_weekend AS
SELECT
    pickup_dayofweek,
    is_weekend,
    COUNT(*) AS trip_count
FROM final_taxi_features
GROUP BY pickup_dayofweek, is_weekend
ORDER BY pickup_dayofweek;

-- ------------------------------------------------
-- STEP 11: CREATE INDICES FOR PERFORMANCE
-- ------------------------------------------------

CREATE INDEX idx_pickup_datetime ON final_taxi_features(pickup_datetime);
CREATE INDEX idx_pickup_hour ON final_taxi_features(pickup_hour);
CREATE INDEX idx_pickup_lat_lon_bucket ON final_taxi_features(pickup_lat_bucket, pickup_lon_bucket);
CREATE INDEX idx_dropoff_lat_lon_bucket ON final_taxi_features(dropoff_lat_bucket, dropoff_lon_bucket);

-- ================================================
-- PIPELINE COMPLETE! EXPORT final_taxi_features TABLE
-- ================================================

