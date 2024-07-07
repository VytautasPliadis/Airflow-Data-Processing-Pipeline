CREATE DATABASE test;

CREATE VIEW daily_new_data_engineer_ads AS
SELECT
    DATE(timestamp) AS metric_date,
    COUNT(*) AS new_data_engineer_ads
FROM
    jobs
WHERE
    title ILIKE '%data engineer%'
GROUP BY
    DATE(timestamp);

