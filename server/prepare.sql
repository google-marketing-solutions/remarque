CREATE OR REPLACE TABLE `{destination_table}` AS
WITH
  commonData AS (
    SELECT
      device.advertising_id AS user,
      geo.country AS country,
      event_name,
      event_timestamp,
      event_date,
      traffic_source.source AS acquisition_source,
      traffic_source.medium AS acquisition_medium,
      device.mobile_brand_name,
      device.operating_system_version
    FROM
      `{source_table}`
    WHERE
      device.category = 'mobile'
      AND device.operating_system = 'Android'
      AND device.advertising_id IS NOT NULL
      AND device.advertising_id NOT IN ('', '00000000-0000-0000-0000-000000000000')
      AND _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND app_info.id = '{app_id}'
  ),
  firstOpens AS (
    SELECT
      DISTINCT user,
      DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y%m%d', event_date), DAY) days_since_install,
      acquisition_source,
      acquisition_medium,
      mobile_brand_name,
      operating_system_version,
      ROW_NUMBER() OVER(PARTITION BY user ORDER BY event_timestamp DESC) r
    FROM
      commonData
    WHERE
      event_name = 'first_open'
      AND country IN ({countries})
  ),
  all_events AS (
    SELECT
      user,
      ARRAY_AGG(event_name) events,
      SUM(IF(event_name = 'session_start', 1, 0)) n_sessions
    FROM
      commonData
    WHERE
      event_name IN ({all_events_list})
    GROUP BY
      1
  )
SELECT
  DISTINCT c.user,
  f.mobile_brand_name as brand,
  f.operating_system_version as osv,
  f.days_since_install,
  IFNULL(f.acquisition_source,'') || '_' || IFNULL(f.acquisition_medium,'') as src,
  i.n_sessions
FROM
  commonData c
  JOIN firstOpens f ON c.user = f.user AND f.r = 1
  JOIN all_events i ON f.user = i.user
WHERE
  {SEARCH_CONDITIONS}
;
