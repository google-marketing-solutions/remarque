"""
 Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple, Literal
from google.auth import credentials
from google.cloud import bigquery
#from google.cloud.exceptions import NotFound  # type: ignore
from google.api_core.exceptions import NotFound # type: ignore
import numpy as np
import pandas as pd
import pandas_gbq
import country_converter as coco
from itertools import groupby
from collections import namedtuple

from logger import logger
from config import Config, ConfigTarget, Audience
from bigquery_utils import CloudBigQueryUtils
from utils import format_duration

AudienceLog = namedtuple(
  'AudienceLog',
  ['name', 'date', 'job', 'user_count', 'new_user_count', 'new_control_user_count', 'test_user_count', 'control_user_count', 'total_user_count', 'total_control_user_count']
  )

class DataGateway:
  """Object for loading and udpating data in Database
  (which is BigQuery but it should be hidden from consumers)"""

  def __init__(self, config: Config, credentials: credentials.Credentials) -> None:
    self.config = config
    logger.debug(f'DataGateway.init: project_id={config.project_id}, bq_location={config.bq_dataset_location}')
    self.bq_client = bigquery.Client(project=config.project_id,
                                    credentials=credentials,
                                    location=config.bq_dataset_location)
    self.bq_client.default_project = config.project_id
    self.bq_utils = CloudBigQueryUtils(self.bq_client)
    self.credentials = credentials


  def initialize(self, target: ConfigTarget):
    ds = self.bq_utils.create_dataset_if_not_exists(target.bq_dataset_id, self.config.bq_dataset_location)
    schema = [
       bigquery.SchemaField(name="name", field_type="STRING", mode="REQUIRED"),
       bigquery.SchemaField(name="app_id", field_type="STRING", mode="REQUIRED"),
       bigquery.SchemaField(name="table_name", field_type="STRING", mode="NULLABLE"),
       bigquery.SchemaField(name="countries", field_type="STRING", mode="REPEATED"),
       bigquery.SchemaField(name="events_include", field_type="STRING", mode="REPEATED"),
       bigquery.SchemaField(name="events_exclude", field_type="STRING", mode="REPEATED"),
       bigquery.SchemaField(name="days_ago_start", field_type="INT64", mode="REQUIRED"),
       bigquery.SchemaField(name="days_ago_end", field_type="INT64", mode="REQUIRED"),
       bigquery.SchemaField(name="user_list", field_type="STRING"),
       bigquery.SchemaField(name="created", field_type="TIMESTAMP"),
       bigquery.SchemaField(name="mode", field_type="STRING"),
    ]
    table_name = f"{target.bq_dataset_id}.audiences"
    self._ensure_table(table_name, schema)

    table_name = f"{target.bq_dataset_id}.audiences_log"
    schema = [
      bigquery.SchemaField(name="name", field_type="STRING", mode="REQUIRED", description="Audience name from audiences table"),
      bigquery.SchemaField(name="date", field_type="TIMESTAMP", mode="REQUIRED", description="Date when segment was uploaded to Google Ads"),
      bigquery.SchemaField(name="job", field_type="STRING", mode="REQUIRED", description="Google Ads offline job resource name"),
      bigquery.SchemaField(name="user_count", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(name="new_user_count", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(name="new_control_user_count", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(name="test_user_count", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(name="control_user_count", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(name="total_user_count", field_type="INT64", mode="REQUIRED"),
      bigquery.SchemaField(name="total_control_user_count", field_type="INT64", mode="REQUIRED"),
    ]
    self._ensure_table(table_name, schema)


  def _ensure_table(self, table_name, expected_schema):
    table_ref = bigquery.TableReference.from_string(table_name, self.config.project_id)
    try:
      table = self.bq_client.get_table(table_ref)
      logger.debug(f"Initialize: table {table_name} exists")

      current_schema = table.schema
      schema_fields = []
      for expected_field in expected_schema:
        current_field = None
        for field in current_schema:
          if field.name == expected_field.name:
            current_field = field
            break

        if current_field is None:
          schema_fields.append(expected_field)
        elif (
          expected_field.field_type != current_field.field_type or
          expected_field.mode != current_field.mode
        ):
          schema_fields.append(expected_field)

      for current_field in current_schema:
        if not any(field.name == current_field.name for field in expected_schema):
          schema_fields.append(current_field)

      if schema_fields:
        table.schema = expected_schema
        try:
          table = self.bq_client.update_table(table, ["schema"])
          logger.info(f'Table {table_name} schema updated')
        except Exception as e:
          if getattr(e, "errors", None) and e.errors[0]:
            if e.errors[0].get("debugInfo", None) and e.errors[0]["debugInfo"].startswith("[SCHEMA_INCOMPATIBLE]"):
              logger.info(f'Table {table_name} schema is incompatible and the table has to be recreated')
              self.bq_client.delete_table(table, not_found_ok=True)
              table = bigquery.Table(table_ref, schema=expected_schema)
              self.bq_client.create_table(table)
              return
          raise

    except NotFound:
      logger.debug(f"Initialize: Creating '{table_name}' table")
      #self.bq_client.create_table(table_name,)

      table = bigquery.Table(table_ref, schema=expected_schema)
      self.bq_client.create_table(table)


  def execute_query(self, query: str) -> list[dict]:
    ts_start = datetime.now()
    logger.debug(f'Executing SQL query: {query}')
    results = self.bq_client.query(query).result()
    # TODO: check exception: e.errors[0].reason == 'invalidQuery'
    fields = [field.name for field in results.schema]
    data_list = []

    for row in results:
        row_dict = {}
        for i, value in enumerate(row):
            row_dict[fields[i]] = value
        data_list.append(row_dict)

    elapsed = datetime.now() - ts_start
    logger.debug(f'Query executed successfully (elapsed {format_duration(elapsed)})')
    return data_list


  def get_ga4_table_name(selft, target: ConfigTarget, wildcard = False):
    ga_fqn = f'{target.ga4_project}.{target.ga4_dataset}.{target.ga4_table}'
    if wildcard and ga_fqn[-2:] != '_*':
       ga_fqn += '_*'
    return ga_fqn


  def get_ga4_stats(self, target: ConfigTarget, days_ago_start: int, days_ago_end: int):
    if days_ago_start < days_ago_end:
       raise ValueError('days_ago_start should be greater than days_ago_end')

    ga_table = self.get_ga4_table_name(target, True)
    # load events stats
    query = f"""
SELECT
  app_info.id AS app_id,
  event_name as event,
  count(1) as count
FROM
  `{ga_table}`
WHERE
    device.category = 'mobile'
    AND device.operating_system = 'Android'
    AND device.advertising_id IS NOT NULL
    AND device.advertising_id NOT IN ('', '00000000-0000-0000-0000-000000000000')
    AND _TABLE_SUFFIX BETWEEN
      format_date('%Y%m%d', date_sub(CURRENT_DATE(), INTERVAL {str(int(days_ago_start))} DAY))
      AND
      format_date('%Y%m%d', date_sub(CURRENT_DATE(), INTERVAL {str(int(days_ago_end))} DAY))
GROUP BY app_info.id, event_name
ORDER BY 1, 3 DESC
"""
    events_stat = self.execute_query(query)
    #pprint(events_stat)
    app_ids = set()
    events_stat_dict = {}
    for app_id, group in groupby(sorted(events_stat, key=lambda x: x['app_id']), key=lambda x: x['app_id']):
        events_stat_dict[app_id] = list(group)
        app_ids.add(app_id)

    query = f"""
SELECT
  app_info.id as app_id,
  geo.country AS country,
  '' AS country_code,
  count(DISTINCT device.advertising_id) as count
FROM
  `{ga_table}`
WHERE
    device.category = 'mobile'
    AND device.operating_system = 'Android'
    AND device.advertising_id IS NOT NULL
    AND device.advertising_id NOT IN ('', '00000000-0000-0000-0000-000000000000')
    AND _TABLE_SUFFIX BETWEEN
      format_date('%Y%m%d', date_sub(CURRENT_DATE(), INTERVAL {str(int(days_ago_start))} DAY))
      AND
      format_date('%Y%m%d', date_sub(CURRENT_DATE(), INTERVAL {str(int(days_ago_end))} DAY))
GROUP BY app_info.id, geo.country
HAVING country is not null AND country != ''
ORDER BY 1, 3 DESC
"""
    countries_stat = self.execute_query(query)
    #pprint(countries_stat)
    countries_stat_dict = {}
    for app_id, group in groupby(sorted(countries_stat, key=lambda x: x['app_id']), key=lambda x: x['app_id']):
        countries = list(group)
        for country in countries:
          code = coco.convert(names = [country['country']], to = 'ISO2', not_found=None)
          country['country_code'] = code
          if code == country['country']:
            logger.warning(f"Could not find country by its name {country['country']}")
        countries_stat_dict[app_id] = countries

    return {
        "app_ids": sorted(list(app_ids)),
        "events": events_stat_dict,
        "countries": countries_stat_dict
      }


  def get_audiences(self, target: ConfigTarget) -> list[Audience]:
    query = f"""
SELECT
  name, app_id, table_name, countries, events_include, events_exclude, days_ago_start, days_ago_end, user_list, created, mode
FROM `{target.bq_dataset_id}.audiences`"""
    rows = self.execute_query(query)
    audiences = []
    for row in rows:
      audience = Audience()
      audience.name = row['name']
      audience.app_id = row['app_id']
      audience.table_name = row['table_name']
      audience.countries = row['countries']
      audience.events_include = row['events_include']
      audience.events_exclude = row['events_exclude']
      audience.days_ago_start = row['days_ago_start']
      audience.days_ago_end = row['days_ago_end']
      audience.user_list = row['user_list']
      audience.created = row['created']
      audience.mode = row['mode']
      audiences.append(audience)
    return audiences


  def update_audiences(self, target: ConfigTarget, audiences: list[Audience]):
    table_name = f"{target.bq_dataset_id}.audiences"
    audiences = audiences or []
    audiences_old = self.get_audiences(target)
    #rows = list(self.bq_client.list_rows(table_name))
    logger.debug("Current audiences:")
    logger.debug(audiences_old)
    to_remove: list[Audience] = []
    to_create: list[Audience] = []
    to_update: list[Audience] = []

    for new in audiences:
      name = new.name
      old = next((old for old in audiences_old if old.name == name), None)
      if old:
        to_update.append(new)
      else:
        to_create.append(new)

    for old in audiences_old:
      new = next((new for new in audiences if new.name == old.name), None)
      if new is None:
        to_remove.append(old)

    logger.debug('audiences to create: ')
    logger.debug(to_create)
    logger.debug('audiences to update: ')
    logger.debug(to_update)
    logger.debug('audiences to remove: ')
    logger.debug(to_remove)

    for i in to_create + to_update:
      name = i.name
      # TODO: 1) validate name 2) generate table name
      if not i.table_name:
        i.table_name = 'audience_' + i.name

    selects = []
    for i in to_create + to_update:
      selects.append(
        f"""SELECT '{i.name}' name, '{i.app_id}' app_id,
  {"'" + i.table_name + "'" if i.table_name is not None else "NULL"} table_name,
  {i.countries} countries,
  {i.events_include} events_include, {i.events_exclude} events_exclude,
  {i.days_ago_start} days_ago_start, {i.days_ago_end} days_ago_end,
  {"'" + i.user_list + "'" if i.user_list is not None else "CAST(NULL as STRING)"} user_list,
  {"'" + i.mode + "'"} mode
""")
    sql_selects = "\nUNION ALL\n".join(selects)
    query = f"""
MERGE `{table_name}` t
USING (
{sql_selects}
) s
ON t.name = s.name
WHEN MATCHED THEN
  UPDATE SET t.app_id = s.app_id,
    t.countries = s.countries,
    t.events_include = s.events_include,
    t.events_exclude = s.events_exclude,
    t.days_ago_start = s.days_ago_start,
    t.days_ago_end = s.days_ago_end,
    t.user_list = CASE
      when s.user_list IS NULL THEN t.user_list
      ELSE s.user_list
    END,
    t.mode = s.mode
WHEN NOT MATCHED THEN
  INSERT (name, app_id, table_name, countries, events_include, events_exclude, days_ago_start, days_ago_end, created, mode)
  VALUES (s.name, s.app_id, s.table_name, s.countries, s.events_include, s.events_exclude, s.days_ago_start, s.days_ago_end, CURRENT_TIMESTAMP(), s.mode)
"""
    logger.debug(query)
    self.bq_client.query(query).result()

    # delete removed audiences
    sql_names_to_delete = ",".join([ "'" + item.name + "'" for item in to_remove])
    if sql_names_to_delete:
      query = f"""DELETE FROM `{table_name}` WHERE name in ({sql_names_to_delete})"""
      logger.debug(query)
      self.bq_client.query(query).result()

    names_to_delete = [ item.name for item in to_remove]
    result = {"deleted": names_to_delete}

    for audience in to_remove:
      self._onRemovedAudience(target, audience)
    return result


  def _onRemovedAudience(self, target: ConfigTarget, audience: dict):
    table_name = target.bq_dataset_id + '.' + audience.table_name
    self.bq_client.delete_table(table_name, not_found_ok=True)


  def calculate_users_for_audiences(self, target: ConfigTarget, audience: Audience):
    pass


  def fetch_audience_users(self, target: ConfigTarget, audience: Audience, suffix: str = None):
    """Segment an audience - takes a audience desrciption and fetches the users from GA4 events according to the conditions.
    """
    days_ago_start = audience.days_ago_start
    days_ago_end = audience.days_ago_end
    day_start = (datetime.now() - timedelta(days=days_ago_start)).strftime("%Y%m%d")
    day_end = (datetime.now() - timedelta(days=days_ago_end)).strftime("%Y%m%d")
    logger.debug(f"Creating user segment for time window: {day_start} - {day_end}")

    countries = ",".join([f"'{c}'" for c in audience.countries])
    events_include = audience.events_include or []
    events_exclude = audience.events_exclude or []
    if not 'first_open' in events_include:
      events_include = ['first_open'] + events_include
    if not 'app_remove' in events_exclude:
      events_exclude = ['app_remove'] + events_exclude

    all_events = events_include + events_exclude
    if not 'session_start' in all_events:
      all_events = ['session_start'] + all_events

    all_events_list = ", ".join([f"'{event}'" for event in all_events])
    search_condition = \
      " AND ".join([f"'{event}' IN UNNEST(i.events)" for event in events_include]) + \
      " AND " + \
      " AND ".join([f"'{event}' NOT IN UNNEST(i.events)" for event in events_exclude])

    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    filename = os.path.join(script_dir, 'prepare.sql')
    with open(filename) as f:
      query = f.read()

    # create table
    suffix = datetime.now().strftime("%Y%m%d") if suffix is None else suffix
    destination_table = target.bq_dataset_id + '.' + audience.table_name + "_" + suffix
    query = query.format(**{
      "destination_table": destination_table,
      "source_table": self.get_ga4_table_name(target, True),
      "day_start": day_start,
      "day_end": day_end,
      "app_id": audience.app_id,
      "countries": countries,
      "all_events_list": all_events_list,
      "SEARCH_CONDITIONS": search_condition
    })
    # TODO: we can add a column is_test into prepare.sql and update it after sampling,
    # but before doing it make sure do_sampling function correctly handles additional columns (hint: it doesn't)!
    self.execute_query(query)

    df = self.load_sampled_users(destination_table)
    return df


  def load_sampled_users(self, audience_table_name: str):
    query = f"""
SELECT
  user,
  brand,
  osv,
  days_since_install,
  src,
  n_sessions
FROM `{audience_table_name}`
"""
    #users = self.execute_query(query)
    logger.debug(f'Executing SQL query: {query}')
    df = pd.read_gbq(
        query=query,
        project_id=self.config.project_id,
        credentials=self.credentials
    )

    return df


  def _get_user_segment_table_full_name(self, target: ConfigTarget,
                                        audience_table_name,
                                        group_name: Literal['test'] | Literal['control'] = 'test',
                                        suffix: str = None):
    bq_dataset_id = target.bq_dataset_id
    suffix = datetime.now().strftime("%Y%m%d") if suffix is None else suffix
    test_table_name =f'{bq_dataset_id}.{audience_table_name}_{group_name}_{suffix}'
    return test_table_name


  def save_sampled_users(self, target: ConfigTarget, audience: Audience,
                         users_test: pd.DataFrame, users_control: pd.DataFrame,
                         suffix: str = None):
    """Save sampled users from two DataFrames into two new tables (_test and _control)"""
    project_id = self.config.project_id
    # TODO: we might want to update original rows in audience_table
    # to flip is_test field to true/false basing on existence in
    # either users_test or users_control dataframes
    test_table_name = self._get_user_segment_table_full_name(target, audience.table_name, 'test', suffix)
    # add 'status' column (empty for all rows)
    users_test = users_test.assign(status=None).astype({"status": 'Int64'})
    pandas_gbq.to_gbq(users_test, test_table_name, project_id, if_exists='replace')

    control_table_name = self._get_user_segment_table_full_name(target, audience.table_name, 'control', suffix)
    pandas_gbq.to_gbq(users_control, control_table_name, project_id, if_exists='replace')
    logger.info(f'Sampled users for audience {audience.name} saved to {test_table_name}/{control_table_name} tables')


  def load_audience_segment(self, target: ConfigTarget, audience: Audience, group_name: Literal['test'] | Literal['control'] = 'test', suffix: str = None) -> list[str]:
    """Loads test users of a given audience for a particular segment (by default - today)"""
    table_name = self._get_user_segment_table_full_name(target, audience.table_name, group_name, suffix)
    query = f"""SELECT user FROM `{table_name}`"""
    rows = self.execute_query(query)
    users = [row['user'] for row in rows]
    return users


  def update_audience_segment_status(self, target: ConfigTarget, audience: Audience,
                                     suffix: str, failed_users: list[str]):
    """Update users statuses in a segment table (by deafult - for today)
        Returns:
          tuple (new_user_count, test_user_count, control_user_count)
    """
    # Originally all users in the segment table ({audience_table_name}_test_yyymmdd) have status=NULL
    # We'll update the column to 1 for all except ones in the failed_users list
    table_name = self._get_user_segment_table_full_name(target, audience.table_name, 'test', suffix)
    if not failed_users:
      query = f"""UPDATE `{table_name}` SET status = 1 WHERE true"""
      self.execute_query(query)
    else:
      # NOTE: create a table for failed users, but its name shoud not be one that will be caught by wildcard mask {name}_test_*
      table_name_failed = self._get_user_segment_table_full_name(target, audience.table_name, 'testfailed', suffix)
      schema = [
        bigquery.SchemaField('user', 'INTEGER', mode='REQUIRED')
      ]
      table_ref = self.bq_client.dataset(target.bq_dataset_id).table(table_name_failed)
      table = bigquery.Table(table_ref, schema=schema)
      table = self.bq_client.create_table(table)
      try:
        self.bq_client.insert_rows(table, table_name_failed)
      except BaseException as e:
        logger.error(f"An error occurred while inserting failed user ids into {table_name_failed} table: {e}")
        raise
      # now join failed users from the newly created _failed table with the segment table
      query = f"""
UPDATE `{table_name}` t1
SET status = IF(t2.failed IS NULL, 1, 0)
FROM (
  SELECT t.user as user, f.user as failed
  FROM `{table_name}` t
  LEFT JOIN `{table_name_failed}` f
  USING(user)
) t2
WHERE t1.user = t2.user
      """
      self.execute_query(query)

    # load test and control user counts
    query = f"SELECT COUNT(1) as count FROM `{table_name}`"
    test_user_count = self.execute_query(query)[0]["count"]
    control_table_name = self._get_user_segment_table_full_name(target, audience.table_name, 'control', suffix)
    query = f"SELECT COUNT(1) as count FROM `{control_table_name}`"
    control_user_count = self.execute_query(query)[0]["count"]

    # load new user count
    table_name_prev = self._get_user_segment_table_full_name(target, audience.table_name, 'test', "*")
    suffix = datetime.now().strftime("%Y%m%d") if suffix is None else suffix
    query = f"""SELECT count(DISTINCT t.user) as user_count FROM `{table_name}` t
WHERE NOT EXISTS (
  SELECT * FROM `{table_name_prev}` t0
  WHERE t0._TABLE_SUFFIX != '{suffix}' AND t.user=t0.user
)
    """
    res = self.execute_query(query)
    new_test_user_count = res[0]["user_count"]

    control_table_name_prev = self._get_user_segment_table_full_name(target, audience.table_name, 'control', "*")
    query = f"""SELECT count(DISTINCT t.user) as user_count FROM `{control_table_name}` t
WHERE NOT EXISTS (
  SELECT * FROM `{control_table_name_prev}` t0
  WHERE t0._TABLE_SUFFIX != '{suffix}' AND t.user=t0.user
)
    """
    res = self.execute_query(query)
    new_control_user_count = res[0]["user_count"]
    return test_user_count, control_user_count, new_test_user_count, new_control_user_count


  def update_audiences_log(self, target: ConfigTarget, result: list[AudienceLog]):
    #result[user_list_name] = { "job_resource_name": job_resource_name, "user_count": len(uploaded_users) }
    table_name = f"{target.bq_dataset_id}.audiences_log"
    table = self.bq_client.get_table(table_name)
    self.bq_client.insert_rows(table, result)
    logger.debug(f'Saved audience_log: ')


  def get_audiences_log(self, target: ConfigTarget) -> dict[str, list[AudienceLog]]:
    table_name = f"{target.bq_dataset_id}.audiences_log"
    query = f"""SELECT name, date, job, user_count, new_user_count, new_control_user_count, test_user_count, control_user_count, total_user_count, total_control_user_count
FROM `{table_name}`
ORDER BY name, date
    """
    rows = self.execute_query(query)

    result = {}
    for name, group in groupby(rows, key=lambda x: x['name']):
      log_items = []
      for item in group:
        log_item = AudienceLog(
          item["name"],
          item["date"],
          item["job"],
          item["user_count"],
          item["new_user_count"],
          item["new_control_user_count"],
          item["test_user_count"],
          item["control_user_count"],
          item["total_user_count"],
          item["total_control_user_count"]
          )
        log_items.append(log_item)
      result[name] = log_items
    return result


  def get_user_conversions(self, target: ConfigTarget, audience: Audience,
                           date_start: date = None, date_end: date = None):
    if date_start is None:
      log = self.get_audiences_log(target)
      log_rows = log.get(audience.name, None)
      if log_rows is None:
        # no imports for the audience, then we'll use the audience creation date
        date_start = audience.created
      else:
        # Start listing conversions makes sense from the day when first segment was uploaded to Google Ads
        date_start = min(log_rows, key=lambda i: i.date).date
    if date_end is None:
      date_end = date.today() - timedelta(days=1)

    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    filename = os.path.join(script_dir, 'results.sql')
    with open(filename) as f:
      query = f.read()

    # NOTE: all events that we ignored for sampling (we picked users for whom those events didn't happen)
    # now are our conversions, but except "app_remove"
    conversion_events = [item for item in audience.events_exclude if item != 'app_remove']
    events_list = ", ".join([f"'{event}'" for event in conversion_events])
    ga_table = self.get_ga4_table_name(target, True)
    user_table = target.bq_dataset_id + '.' + audience.table_name
    query = query.format(**{
      "source_table": ga_table,
      "events": events_list,
      "day_start": date_start.strftime("%Y%m%d"),
      "day_end": date_end.strftime("%Y%m%d"),
      "test_users_table": user_table + "_test_*",
      "control_users_table": user_table + "_control_*",
      "date_start": date_start.strftime("%Y-%m-%d"),
      "date_end": date_end.strftime("%Y-%m-%d"),
      "audiences_log": target.bq_dataset_id + ".audiences_log",
      "audience_name": audience.name
    })
    # expect columns: date, cum_test_regs, cum_control_regs, total_user_count, total_control_user_count
    result = self.execute_query(query)
    return result, date_start, date_end
