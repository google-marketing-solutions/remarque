    WITH DailyUserCounts AS (
      SELECT
        _TABLE_SUFFIX as table_date,
        'test' as group_type,
        COUNT(DISTINCT user) as user_count
      FROM `{test_users_table}`
      INNER JOIN `{all_users_table}`
        USING (user)
      WHERE
        _TABLE_SUFFIX IN (
          SELECT FORMAT_DATE('%E4Y%m%d', DATE(`date`)) AS day
          FROM `{audiences_log}`
          WHERE name = '{audience_name}'
        )
        AND app_id = '{app_id}'
        {SEARCH_CONDITIONS}
      GROUP BY _TABLE_SUFFIX

      UNION ALL

      SELECT
        _TABLE_SUFFIX as table_date,
        'control' as group_type,
        COUNT(DISTINCT user) as user_count
      FROM `{control_users_table}`
      INNER JOIN `{all_users_table}`
        USING (user)
      WHERE
        _TABLE_SUFFIX IN (
          SELECT FORMAT_DATE('%E4Y%m%d', DATE(`date`)) AS day
          FROM `{audiences_log}`
          WHERE name = '{audience_name}'
        )
        AND app_id = '{app_id}'
        {SEARCH_CONDITIONS}
      GROUP BY _TABLE_SUFFIX
    )
    SELECT
      PARSE_DATE("%Y%m%d", table_date) AS day,
      SUM(CASE WHEN group_type = 'test' THEN user_count END)
        OVER(ORDER BY table_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_test_user_count,
      SUM(CASE WHEN group_type = 'control' THEN user_count END)
        OVER(ORDER BY table_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_control_user_count
    FROM DailyUserCounts
