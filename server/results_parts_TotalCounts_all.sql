    SELECT DISTINCT
      DATE(`date`) AS day,
      total_user_count,
      total_control_user_count,
      RANK() OVER (PARTITION BY name, format_date('%Y%m%d', `date`) ORDER BY `date` DESC) AS r
    FROM `{audiences_log}`
    WHERE NAME = '{audience_name}'
