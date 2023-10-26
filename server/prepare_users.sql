WITH user_latest_events AS (
  SELECT
    device.advertising_id AS user,
    app_info.id AS app_id,
    geo.country AS country,
    traffic_source.source AS acquisition_source,
    traffic_source.medium AS acquisition_medium,
    device.mobile_brand_name,
    device.operating_system_version,
    event_date,
    event_timestamp,
    ROW_NUMBER() OVER(PARTITION BY device.advertising_id, app_info.id ORDER BY event_timestamp DESC) AS row_num
  FROM
    `{source_table}`
  WHERE
    event_name IN ('session_start', 'first_open')
    AND device.operating_system = 'Android'
    AND device.advertising_id IS NOT NULL
    AND device.advertising_id != ''
    AND device.advertising_id != '00000000-0000-0000-0000-000000000000'
    AND _TABLE_SUFFIX BETWEEN
      FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AND
      FORMAT_DATE('%Y%m%d', CURRENT_DATE())
)
SELECT
  user,
  app_id,
  country,
  acquisition_source,
  acquisition_medium,
  mobile_brand_name,
  operating_system_version,
  DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y%m%d', event_date), DAY) AS days_since_install,
  event_date,
  event_timestamp
FROM
  user_latest_events
WHERE
  row_num = 1
