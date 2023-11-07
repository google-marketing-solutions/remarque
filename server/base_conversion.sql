WITH
  event_table AS (
    SELECT
      device.advertising_id AS user,
      event_name,
      event_date,
    FROM
      `{source_table}`
    WHERE
      _TABLE_SUFFIX BETWEEN '{date_start}' AND '{date_end}'
      AND app_info.id = '{app_id}'
      AND event_name IN ({all_events_list})
  ),
  audience_users AS (
    SELECT
      distinct f.user
    FROM
      `{all_users_table}` f
    WHERE
      {countries_clause}
      AND app_id = '{app_id}'
      AND EXISTS (
        SELECT user from event_table WHERE user=f.user AND event_name IN ({audience_events_list})
          AND event_date BETWEEN '{date_audience_start}' AND '{date_audience_end}'
      )
      AND NOT EXISTS (SELECT user from event_table WHERE user=f.user AND event_name = 'app_remove')
  ),
  converted_users AS (
    SELECT
      distinct f.user
    FROM
      `{all_users_table}` f
    WHERE
      {countries_clause}
      AND app_id = '{app_id}'
      AND EXISTS (
        SELECT user from event_table WHERE user=f.user AND event_name IN ({conversion_events_list})
          AND event_date BETWEEN '{date_conversion_start}' AND  '{date_conversion_end}'
      )
      AND NOT EXISTS (SELECT user from event_table WHERE user=f.user AND event_name = 'app_remove')
  )
  SELECT
    count(a.user) as audience,
    count(c.user) as converted,
    safe_divide(count(c.user), count(a.user)) as cr
  FROM audience_users a
    LEFT JOIN converted_users c USING(user)
