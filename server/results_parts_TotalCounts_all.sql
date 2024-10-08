    SELECT DISTINCT
      DATE(`date`) AS day,
      total_user_count AS total_test_user_count,
      total_control_user_count
    FROM `{audiences_log}`
    WHERE NAME = '{audience_name}'
    QUALIFY RANK() OVER (PARTITION BY format_date('%Y%m%d', `date`) ORDER BY `date` DESC) = 1
