WITH
  event_table AS (
    SELECT
      device.advertising_id AS user,
      ARRAY_AGG(event_name) events,
      SUM(IF(event_name = 'session_start', 1, 0)) n_sessions
    FROM
      `{source_table}`
    WHERE
      _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND app_info.id = '{app_id}'
      AND event_name IN ({all_events_list})
    GROUP BY
      1
  ),
  filtered_users AS (
    SELECT
      f.user,
      f.mobile_brand_name as brand,
      f.operating_system_version as osv,
      f.days_since_install,
      f.acquisition_source,
      f.acquisition_medium,
      n_sessions
    FROM
      event_table
      INNER JOIN `{all_users_table}` f USING (user)
    WHERE
      f.country IN ({countries}) AND
      f.app_id = '{app_id}' AND
      {SEARCH_CONDITIONS}
  )
  SELECT
    DISTINCT user,
    brand,
    osv,
    days_since_install,
    IFNULL(acquisition_source,'') || '_' || IFNULL(acquisition_medium,'') as src,
    n_sessions
  FROM
    filtered_users
