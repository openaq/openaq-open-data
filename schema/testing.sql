-- if you want some test data
\i ../../openaq-db/openaqdb/testing/testing_schema.sql
\set origin '''open-data-tests'''


TRUNCATE sensor_nodes CASCADE;
TRUNCATE measurements CASCADE;


SELECT
    utc_offset
    , array_agg(name) as name
FROM pg_timezone_names t1
JOIN timezones t2 ON (t2.tzid = t1.name)
WHERE name !~ 'posix'
GROUP BY utc_offset
ORDER BY utc_offset

SELECT testing.generate_fake_data(
       3
       , :origin
       , '1hour'
       , '1week'
       , ARRAY[
         'America/Los_Angeles'
         ,'America/New_York'
         , 'America/Denver'
         , 'America/Boise'
         , 'Pacific/Kiritimati'
         , 'Asia/Tehran'
         , 'Australia/Sydney'
         ]
       );

SELECT testing.generate_canary_data(:origin, '1hours', '1week');

-- and after adding data we need to make sure that nightly updates are run
CALL run_updates_full();

-- and then call this again
SELECT reset_export_logs();
