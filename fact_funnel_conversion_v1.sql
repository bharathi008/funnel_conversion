CREATE TABLE fact_conversion_funnel(
  user_id TEXT,
  device_id TEXT,
  initial_referring_domain TEXT,
  region TEXT,
  platform TEXT,
  event_date,
  Home_page_visit_count,
  Store_page_count,
  Checkout_page_count,
  Success_checkout_count
);

DROP TABLE IF EXISTS fact_conversion_funnel;

CREATE TABLE fact_conversion_funnel
AS
WITH stage AS (
  SELECT
    event_time,
    user_id,
    event_type,
    platform,
    country,
    region,
    device_id,
    initial_referring_domain,
    DATE(event_time) AS ds
  FROM
  user_events
  WHERE
    date(event_time) >= DATETIME('2018-01-16  ','-3 day')
),

dedupe_step as (
SELECT
    event_time,
    user_id,
    event_type,
    platform,
    country,
    region,
    device_id,
    initial_referring_domain,
    DATE(event_time) AS ds,
    CAST(strftime('%s', event_time) as integer) - CAST(strftime('%s', LAG(event_time,1,"1999-01-01 01:01:01") OVER (PARTITION BY user_id,event_type,ds ORDER BY event_time )) as integer) as diff
  FROM
    stage
  ORDER BY event_time
  ),

  aggregated AS (
  SELECT
    user_id,
    device_id,
    initial_referring_domain,
    region,
    platform,
    DATE(MIN(event_time)) AS event_date,
    COUNT(CASE WHEN event_type = 'home_page' THEN TRUE END) AS Home_page_visit_count,
    COUNT(CASE WHEN event_type = 'store_ordering_page' THEN TRUE END) AS Store_page_count,
    COUNT(CASE WHEN event_type = 'checkout_page' THEN TRUE END) AS Checkout_page_count,
    COUNT(CASE WHEN event_type = 'checkout_success' THEN TRUE END) AS Success_checkout_count
  FROM
  dedupe_step
  WHERE
    diff > 60
  GROUP by
    user_id,
    device_id,
    initial_referring_domain,
    region,
    platform
    )


   SELECT
    user_id,
    device_id,
    initial_referring_domain,
    region,
    platform,
    event_date,
    Home_page_visit_count,
    Store_page_count,
    Checkout_page_count,
    Success_checkout_count
   FROM aggregated
   WHERE
    event_date >= DATETIME('2018-01-16  ','-2 day')



