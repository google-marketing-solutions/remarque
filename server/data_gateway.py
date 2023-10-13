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
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Literal
from google.auth import credentials
from google.cloud import bigquery
from google.api_core import exceptions
from google.cloud.bigquery.dataset import Dataset
#from google.cloud.exceptions import NotFound  # type: ignore
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

TABLE_USER_NORMALIZED = 'users_normalized'

country_name_to_code_cache = {}

AudienceLog = namedtuple(
  'AudienceLog',
  ['name', 'date', 'job', 'user_count', 'new_user_count', 'new_control_user_count', 'test_user_count', 'control_user_count', 'total_user_count', 'total_control_user_count']
  )

class DataGateway:
  """Object for loading and udpating data in Database
  (which is BigQuery but it should be hidden from consumers)"""

  def __init__(self, config: Config, credentials: credentials.Credentials, target: ConfigTarget,) -> None:
    self.config = config
    logger.debug(f'DataGateway.init: project_id={config.project_id}, bq_location={target.bq_dataset_location if target else None}')
    self.bq_client = bigquery.Client(project=config.project_id,
                                    credentials=credentials,
                                    location=target.bq_dataset_location if target else None)
    self.bq_client.default_project = config.project_id
    self.bq_utils = CloudBigQueryUtils(self.bq_client)
    self.credentials = credentials

  def _recreate_dataset(self, dataset_id, dataset_location, backup_existing_tables=True):
    # Construct a BigQuery client object.
    fully_qualified_dataset_id = f'{self.config.project_id}.{dataset_id}'
    to_create = False
    if dataset_location == 'europe':
      dataset_location = 'eu'
    ds_backup = None
    try:
      ds: Dataset = self.bq_client.get_dataset(fully_qualified_dataset_id)
      if ds.location and dataset_location and ds.location.lower() != dataset_location.lower() or \
         not ds.location and dataset_location or \
         ds.location and not dataset_location:
        # the current dataset's location and desired one are different, we need to recreate the dataset
        logger.info(
            f'Existing dataset needs to be recreated due to different locations (current: {ds.location}, needed: {dataset_location}).')
        # but before doing so let's copy everything aside
        if backup_existing_tables:
          logger.debug(f"Backuping up existing tables in the {dataset_id}")
          # we'll create a backup dataset in the same location as the current one
          ds_backup = bigquery.Dataset(fully_qualified_dataset_id + '_backup_' + datetime.now().strftime("%Y%m%d_%H%M"))
          ds_backup.location = ds.location
          ds_backup = self.bq_client.create_dataset(ds_backup, True)
          i=0
          for t in self.bq_client.list_tables(ds):
            self.bq_client.copy_table(ds.dataset_id + '.' + t.table_id, ds_backup.dataset_id + '.' + t.table_id, location=ds.location)
            i+=1
          logger.debug(f"{i} tables were copied to {ds_backup.dataset_id}")
        self.bq_client.delete_dataset(ds, True)
        logger.debug(f"Dataset {ds.dataset_id} deleted")
        to_create = True
      else:
        logger.info(f'Dataset {fully_qualified_dataset_id} already exists in {dataset_location}.')
    except exceptions.NotFound as e:
      logger.warn(e)
      logger.info(f'Dataset {fully_qualified_dataset_id} is not found in {dataset_location}.')
      to_create = True
    if to_create:
      dataset = bigquery.Dataset(fully_qualified_dataset_id)
      dataset.location = dataset_location
      ds = self.bq_client.create_dataset(dataset)
      logger.info(f'Dataset {fully_qualified_dataset_id} created in {dataset_location}.')
      # copy tables from backup dataset back
      # NOTE: looks like copying won't work between locations anyway
      # if ds_backup:
      #   logger.debug(f'Copying tables from backup dataset {ds_backup.dataset_id} back to {ds.dataset_id}')
      #   for t in self.bq_client.list_tables(ds_backup):
      #     self.bq_client.copy_table(ds_backup.dataset_id + '.' + t.table_id, ds.dataset_id + '.' + t.table_id)

    return ds


  def initialize(self, target: ConfigTarget):
    bq_dataset_id = target.bq_dataset_id
    bq_dataset_location = target.bq_dataset_location
    self.bq_client = bigquery.Client(project=self.config.project_id,
                                    credentials=self.credentials,
                                    location=bq_dataset_location)

    ds = self._recreate_dataset(bq_dataset_id, bq_dataset_location, True)
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
       bigquery.SchemaField(name="query", field_type="STRING", mode="NULLABLE"),
       bigquery.SchemaField(name="ttl", field_type="INT64", mode="NULLABLE"),
    ]
    table_name = f"{bq_dataset_id}.audiences"
    self._ensure_table(table_name, schema)

    table_name = f"{bq_dataset_id}.audiences_log"
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

    # update user segments tables for test users (adding ttl)
    schema = [
        bigquery.SchemaField(name="user", field_type="STRING"),
        bigquery.SchemaField(name="status", field_type="INT64"),
        bigquery.SchemaField(name="ttl", field_type="INT64"),
    ]
    audiences = self.get_audiences(target)
    for audience in audiences:
      if audience.table_name:
        query = f"SELECT table_name FROM {bq_dataset_id}.INFORMATION_SCHEMA.TABLES WHERE table_name like '{audience.table_name}_test_%' ORDER BY 1"
        rows = self.execute_query(query)
        for row in rows:
          table_name = row['table_name']
          self._ensure_table(f"{bq_dataset_id}.{table_name}", schema, strict=False)


  def _ensure_table(self, table_name, expected_schema, strict = False):
    table_ref = bigquery.TableReference.from_string(table_name, self.config.project_id)
    try:
      table = self.bq_client.get_table(table_ref)
      logger.debug(f"Initialize: table {table_name} found")

      current_schema = table.schema
      updated_fields = []
      for expected_field in expected_schema:
        current_field = None
        for field in current_schema:
          if field.name == expected_field.name:
            current_field = field
            break

        expected_field_type = expected_field.field_type
        if expected_field_type == 'INT64':
          expected_field_type = 'INTEGER'
        if current_field is None:
          updated_fields.append(expected_field)
        elif (
          expected_field_type != current_field.field_type or
          expected_field.mode != current_field.mode
        ):
          updated_fields.append(expected_field)

      deleted_fields = []
      for current_field in current_schema:
        if not any(field.name == current_field.name for field in expected_schema):
          deleted_fields.append(current_field)

      if deleted_fields:
        # drop columns (update_table doesn't support removing columns)
        logger.debug(f"Removing excess columns for {table_name}: {deleted_fields}")
        for field in deleted_fields:
          sql = f"ALTER TABLE `{table_name}` DROP COLUMN {field.name}"
          self.execute_query(sql)
        # refresh the table as w/o it we'll have 'PRECONDITION_FAILED: 412' error on update_table
        table = self.bq_client.get_table(table_ref)

      if updated_fields:
        table.schema = expected_schema
        try:
          logger.debug(f"Updating table {table_name} with new schema:\n {updated_fields}")
          table = self.bq_client.update_table(table, ["schema"])
          logger.info(f'Table {table_name} schema updated')
        except Exception as e:
          logger.error(e)
          if getattr(e, "errors", None) and e.errors[0]:
            if e.errors[0].get("debugInfo", None) and e.errors[0]["debugInfo"].startswith("[SCHEMA_INCOMPATIBLE]"):
              logger.info(f'Table {table_name} schema is incompatible and the table has to be recreated')
              backup_table_id = table_name + "_backup"
              try:
                self.bq_client.delete_table(backup_table_id, not_found_ok=True)
              except:
                logger.warning(f"Failed to delete backup table {backup_table_id}")
              try:
                self.bq_client.copy_table(table, backup_table_id)
              except Exception as e:
                logger.warning(f"Failed to backup table before recreating {table.full_table_id}: {e}")
              self.bq_client.delete_table(table, not_found_ok=True)
              table = bigquery.Table(table_ref, schema=expected_schema)
              self.bq_client.create_table(table)
              return
          logger.warning(f"It is not a scheme_incompatible error or we failed to parse it")
          raise
      else:
        logger.debug(f"Table {table_name} has compatible schema")
    except exceptions.NotFound:
      logger.debug(f"Initialize: Creating '{table_name}' table")
      table = bigquery.Table(table_ref, schema=expected_schema)
      self.bq_client.create_table(table)


  def execute_query(self, query: str) -> list[dict]:
    ts_start = datetime.now()
    lines = [f"{i}: {line.strip()}" for i, line in enumerate(query.strip().split('\n'), start=1)]
    query_logged = "\n".join(lines)
    logger.debug(f'Executing SQL query: \n{query_logged}')
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
    logger.debug(f"Loading GA4 stats for target {target.name} (GA4 table {ga_table})")
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
    AND app_info.id IS NOT NULL
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
    logger.debug(f"Loaded event stats per app_id: {len(events_stat)}")
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
    AND app_info.id IS NOT NULL
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
    logger.debug(f"Loaded country stats per app_id: {len(countries_stat)}")

    countries_stat_dict = {}
    ts_start = datetime.now()
    for app_id, group in groupby(sorted(countries_stat, key=lambda x: x['app_id']), key=lambda x: x['app_id']):
        countries = list(group)
        for country in countries:
          country_name = country['country']
          code = country_name_to_code_cache.get(country_name, None)
          if not code:
            code = coco.convert(names = [country['country']], to = 'ISO2', not_found=None)
            country_name_to_code_cache[country_name] = code
          country['country_code'] = code
          if code == country['country']:
            logger.warning(f"Could not find country by its name {country['country']}")
        countries_stat_dict[app_id] = countries
    elapsed = datetime.now() - ts_start
    logger.debug(f"Enriched stats by country with country codes (elapsed {elapsed})")

    return {
        "app_ids": sorted(list(app_ids)),
        "events": events_stat_dict,
        "countries": countries_stat_dict
      }


  def get_audiences(self, target: ConfigTarget) -> list[Audience]:
    query = f"""
SELECT
  name, app_id, table_name, countries, events_include, events_exclude, days_ago_start, days_ago_end, user_list, created, mode, query, ttl
FROM `{target.bq_dataset_id}.audiences`
ORDER BY created
"""
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
      audience.query = row['query']
      audience.ttl = row['ttl']
      audiences.append(audience)
    return audiences


  def update_audiences(self, target: ConfigTarget, audiences: list[Audience]):
    table_name = f"{target.bq_dataset_id}.audiences"
    audiences = audiences or []
    audiences_old = self.get_audiences(target)
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
      i.ensure_table_name()
      if not i.ttl:
        i.ttl = 1

    selects = []
    query_params = []
    idx = 0
    for i in to_create + to_update:
      idx += 1
      query_params.append(bigquery.ScalarQueryParameter("query" + str(idx), "STRING", i.query))
      selects.append(
        f"""SELECT '{i.name}' name, '{i.app_id}' app_id,
  {"'" + i.table_name + "'"} table_name,
  {i.countries} countries,
  {i.events_include} events_include, {i.events_exclude} events_exclude,
  {i.days_ago_start} days_ago_start, {i.days_ago_end} days_ago_end,
  {"'" + i.user_list + "'" if i.user_list is not None else "CAST(NULL as STRING)"} user_list,
  {"'" + i.mode + "'"} mode,
  @query{idx} query,
  {i.ttl} ttl
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
    t.mode = s.mode,
    t.query = s.query,
    t.ttl = s.ttl
WHEN NOT MATCHED THEN
  INSERT (name, app_id, table_name, countries, events_include, events_exclude, days_ago_start, days_ago_end, created, mode, query, ttl)
  VALUES (s.name, s.app_id, s.table_name, s.countries, s.events_include, s.events_exclude, s.days_ago_start, s.days_ago_end, CURRENT_TIMESTAMP(), s.mode, s.query, s.ttl)
"""
    logger.debug(query)
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    self.bq_client.query(query, job_config=job_config).result()

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


  def _read_file(self, filename):
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    filename = os.path.join(script_dir, filename)
    with open(filename) as f:
      query = f.read()
    return query


  def get_audience_sampling_query(self, target: ConfigTarget, audience: Audience):
    days_ago_start = int(audience.days_ago_start)
    days_ago_end = int(audience.days_ago_end)
    day_start = (datetime.now() - timedelta(days=days_ago_start)).strftime("%Y%m%d")
    day_end = (datetime.now() - timedelta(days=days_ago_end)).strftime("%Y%m%d")
    logger.debug(f"Creating user segment for time window: {day_start} - {day_end}")

    countries = ",".join([f"'{c}'" for c in audience.countries])
    events_include = audience.events_include or []
    events_exclude = audience.events_exclude or []
    # if not 'first_open' in events_include:
    #   events_include = ['first_open'] + events_include
    if not 'app_remove' in events_exclude:
      events_exclude = ['app_remove'] + events_exclude

    all_events = events_include + events_exclude
    if not 'session_start' in all_events:
      all_events = ['session_start'] + all_events

    all_events_list = ", ".join([f"'{event}'" for event in all_events])
    search_condition = ""
    if events_include:
      search_condition += " AND ".join([f"'{event}' IN UNNEST(events)" for event in events_include])
    if search_condition:
      search_condition += " AND "
    if events_exclude:
      search_condition += " AND ".join([f"'{event}' NOT IN UNNEST(events)" for event in events_exclude])
    # search_condition = \
    #   " AND ".join([f"'{event}' IN UNNEST(events)" for event in events_include]) + \
    #   " AND " + \
    #   " AND ".join([f"'{event}' NOT IN UNNEST(events)" for event in events_exclude])

    if audience.query:
      query = audience.query
      logger.debug(f"Using customer audience query:\n{query}")
    else:
      query = self._read_file('prepare.sql')

    try:
      query = query.format(**{
        "source_table": self.get_ga4_table_name(target, True),
        "day_start": day_start,
        "day_end": day_end,
        "app_id": audience.app_id,
        "countries_clause": f"f.country IN ({countries}) " if audience.countries else "1=1",
        "countries": countries,
        "all_users_table": target.bq_dataset_id + "." + TABLE_USER_NORMALIZED,
        "all_events_list": all_events_list,
        "SEARCH_CONDITIONS": search_condition
      })
    except KeyError as e:
      raise Exception(f"An error occured during substituting macros into audience query, unknown macro {e} was used")

    return query


  def _create_users_normalized_table(self, target: ConfigTarget):
    query = self._read_file('prepare_users.sql')
    query = query.format(**{
      "source_table": self.get_ga4_table_name(target, True),
    })
    destination_table = target.bq_dataset_id + '.' + TABLE_USER_NORMALIZED
    query = f"CREATE OR REPLACE TABLE `{destination_table}` AS\n" + query
    self.execute_query(query)
    #now we should have `users_normalized` table


  def ensure_user_normalized(self, target: ConfigTarget):
    to_create = False
    creation_time = self.bq_utils.get_table_creation_time(target.bq_dataset_id, TABLE_USER_NORMALIZED)
    if creation_time:
      logger.info(f"user_normalized table exists, creation time: {creation_time}")
      if creation_time.date() != datetime.now(timezone.utc).date():
        # table was created not today
        logger.debug(f"Table user_normalized needs to be recreated")
        to_create = True
    else:
      to_create = True
    if to_create:
      logger.info(f"Recreating user_normalized table")
      self._create_users_normalized_table(target)


  def fetch_audience_users(self, target: ConfigTarget, audience: Audience, suffix: str = None):
    """Segment an audience - takes an audience desrciption and fetches the users
       from GA4 events according to the conditions.

       Returns:
        DataFrame with columns user, brand, osv, days_since_install, src, n_sessions
    """
    suffix = datetime.now().strftime("%Y%m%d") if suffix is None else suffix
    audience.ensure_table_name()
    destination_table = target.bq_dataset_id + '.' + audience.table_name + "_all_" + suffix
    query = self.get_audience_sampling_query(target, audience)
    query = f"CREATE OR REPLACE TABLE `{destination_table}` AS\n" + query

    # TODO: parse error with line:column and add query with numbered lines into exception for easier diagnosis
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


  def load_old_users(self, target: ConfigTarget, audience: Audience):
    table_name_test = self._get_user_segment_table_full_name(target, audience.table_name, "test", '*')
    table_name_ctrl = self._get_user_segment_table_full_name(target, audience.table_name, "control", '*')
    suffix_today = datetime.now().strftime("%Y%m%d")
    query_test = f"""SELECT user FROM `{table_name_test}` WHERE _TABLE_SUFFIX != '{suffix_today}'"""
    query_ctrl = f"""SELECT user FROM `{table_name_ctrl}` WHERE _TABLE_SUFFIX != '{suffix_today}'"""
    # If we're running it for the first time then user segment tables can not exist yet
    # (BadRequest: 400 remarque.audience_XXX_test_* does not match any table.)
    try:
      df_test = pd.read_gbq(
          query=query_test,
          project_id=self.config.project_id,
          credentials=self.credentials
      )
      df_ctrl = pd.read_gbq(
          query=query_ctrl,
          project_id=self.config.project_id,
          credentials=self.credentials
      )
    except BaseException as e:
      # we expect an pd.GenericGBQException wrapped an exceptions.BadRequest but better be on the safe side to catch all
      logger.info(f"Loading of users from previous days failed: {e}")
      df_test = pd.DataFrame(columns=['user'])
      df_ctrl = pd.DataFrame(columns=['user'])

    return df_test, df_ctrl


  def _get_user_segment_tables(self, target: ConfigTarget,
                               audience_table_name,
                               group_name: Literal['test'] | Literal['control'] = 'test',
                               suffix: str = None,
                               include_dataset = False):
    bq_dataset_id = target.bq_dataset_id
    query = f"SELECT table_name FROM {bq_dataset_id}.INFORMATION_SCHEMA.TABLES WHERE table_name LIKE '{audience_table_name}_{group_name}_%' ORDER BY 1 DESC"
    rows = self.execute_query(query)
    tables = [row['table_name'] for row in rows]
    if include_dataset:
      return [f"{bq_dataset_id}.{t}" for t in tables]
    return tables

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
    if len(users_test) == 0:
      schema = [
        bigquery.SchemaField(name="user", field_type="STRING"),
        bigquery.SchemaField(name="status", field_type="INT64"),
        bigquery.SchemaField(name="ttl", field_type="INT64"),
      ]
      self._ensure_table(test_table_name, schema)
    else:
      # add 'status' column (empty for all rows)
      users_test = users_test.assign(status=None).astype({"status": 'Int64'})
      # add 'ttl' column with audience's initial ttl
      users_test = users_test.assign(ttl=audience.ttl).astype({"ttl": 'Int64'})
      pandas_gbq.to_gbq(users_test[['user', 'status', 'ttl']], test_table_name, project_id, if_exists='replace')

    control_table_name = self._get_user_segment_table_full_name(target, audience.table_name, 'control', suffix)
    if len(users_control) == 0:
      schema = [
        bigquery.SchemaField(name="user", field_type="STRING"),
      ]
      self._ensure_table(control_table_name, schema)
    else:
      pandas_gbq.to_gbq(users_control[['user']], control_table_name, project_id, if_exists='replace')

    logger.info(f'Sampled users for audience {audience.name} saved to {test_table_name}/{control_table_name} tables')

    # add test users from yesterday with TTL>1 into today's test users
    if audience.ttl > 0:
      tables = self._get_user_segment_tables(target, audience.table_name, 'test', suffix, include_dataset=True)
      if tables:
        try:
          idx = tables.index(test_table_name)
          if idx != len(tables) - 1:
            test_table_name_yesterday = tables[idx+1]
            logger.debug(f"Adding users with TTL>1 from yesterday ({test_table_name_yesterday})")
            query = f"""
  INSERT INTO `{test_table_name}` (user, ttl)
  SELECT user, ttl-1 FROM `{test_table_name_yesterday}` t1
  WHERE
    NOT EXISTS (SELECT user FROM `{test_table_name}` WHERE user=t1.user)
    AND ttl>1
  """
            res = self.execute_query(query)
            logger.debug(f"Added test users from previous day with TTL>1")
        except ValueError:
          pass



  def load_audience_segment(self, target: ConfigTarget, audience: Audience, group_name: Literal['test'] | Literal['control'] = 'test', suffix: str = None) -> list[str]:
    """Loads test users of a given audience for a particular segment (by default - today)"""
    table_name = self._get_user_segment_table_full_name(target, audience.table_name, group_name, suffix)
    query = f"""SELECT user FROM `{table_name}`"""
    try:
      rows = self.execute_query(query)
      users = [row['user'] for row in rows]
    except exceptions.NotFound:
      logger.debug(f"Table '{table_name}' not found (audience segment is empty)")
      users = []
    return users


  def update_audience_segment_status(self, target: ConfigTarget, audience: Audience,
                                     suffix: str, failed_users: list[str]):
    """Update users statuses in a segment table (by default - for today)
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
        bigquery.SchemaField('user', 'STRING', mode='REQUIRED')
      ]
      table_ref = bigquery.TableReference.from_string(table_name_failed, self.config.project_id)
      table = bigquery.Table(table_ref, schema=schema)
      table = self.bq_client.create_table(table, exists_ok=True)
      rows_to_insert = [{'user': user_id} for user_id in failed_users]
      try:
        self.bq_client.insert_rows(table, rows_to_insert)
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
