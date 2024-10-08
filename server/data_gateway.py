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
import re
from datetime import date, datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from typing import Literal
from google.auth import credentials
from google.cloud import bigquery
from google.api_core import exceptions, retry
from google.cloud.bigquery.dataset import Dataset
#from google.cloud.exceptions import NotFound  # type: ignore
import pandas as pd
import pandas_gbq
import country_converter as coco
from itertools import groupby

from logger import logger
from config import Config, ConfigTarget, AppNotInitializedError
from models import Audience, AudienceLog
from bigquery_utils import CloudBigQueryUtils
from utils import format_duration

TABLE_USERS_NORMALIZED = 'users_normalized'

country_name_to_code_cache = {}


class QueryExecutionError(Exception):

  def __init__(self, msg: str = None, query: str = None) -> None:
    super().__init__(msg)
    self.query = query


class TableSchemas:
  """Container for DB tables schemas"""
  audiences = [
      bigquery.SchemaField(name='name', field_type='STRING', mode='REQUIRED'),
      bigquery.SchemaField(name='app_id', field_type='STRING', mode='REQUIRED'),
      bigquery.SchemaField(
          name='table_name', field_type='STRING', mode='NULLABLE'),
      bigquery.SchemaField(
          name='countries', field_type='STRING', mode='REPEATED'),
      bigquery.SchemaField(
          name='events_include', field_type='STRING', mode='REPEATED'),
      bigquery.SchemaField(
          name='events_exclude', field_type='STRING', mode='REPEATED'),
      bigquery.SchemaField(
          name='days_ago_start', field_type='INT64', mode='REQUIRED'),
      bigquery.SchemaField(
          name='days_ago_end', field_type='INT64', mode='REQUIRED'),
      bigquery.SchemaField(name='user_list', field_type='STRING'),
      bigquery.SchemaField(name='created', field_type='TIMESTAMP'),
      bigquery.SchemaField(name='mode', field_type='STRING'),
      bigquery.SchemaField(name='query', field_type='STRING', mode='NULLABLE'),
      bigquery.SchemaField(name='ttl', field_type='INT64', mode='NULLABLE'),
      bigquery.SchemaField(
          name='split_ratio', field_type='FLOAT64', mode='NULLABLE'),
  ]
  audiences_log = [
      bigquery.SchemaField(
          name='name',
          field_type='STRING',
          mode='REQUIRED',
          description='Audience name from audiences table'),
      bigquery.SchemaField(
          name='date',
          field_type='TIMESTAMP',
          mode='REQUIRED',
          description='Date when segment was uploaded to Google Ads'),
      bigquery.SchemaField(
          name='job',
          field_type='STRING',
          mode='NULLABLE',
          description='Google Ads offline job resource name'),
      bigquery.SchemaField(
          name='user_count', field_type='INT64', mode='REQUIRED'),
      bigquery.SchemaField(
          name='new_user_count', field_type='INT64', mode='REQUIRED'),
      bigquery.SchemaField(
          name='new_control_user_count', field_type='INT64', mode='REQUIRED'),
      bigquery.SchemaField(
          name='test_user_count', field_type='INT64', mode='REQUIRED'),
      bigquery.SchemaField(
          name='control_user_count', field_type='INT64', mode='REQUIRED'),
      bigquery.SchemaField(
          name='total_user_count', field_type='INT64', mode='REQUIRED'),
      bigquery.SchemaField(
          name='total_control_user_count', field_type='INT64', mode='REQUIRED'),
  ]
  daily_test_users = [
      bigquery.SchemaField(name='user', field_type='STRING'),
      bigquery.SchemaField(name='status', field_type='INT64'),
      bigquery.SchemaField(name='ttl', field_type='INT64'),
  ]
  daily_control_users = [
      bigquery.SchemaField(name='user', field_type='STRING'),
      bigquery.SchemaField(name='ttl', field_type='INT64'),
  ]


class DataGateway:
  """Object for loading and udpating data in Database
  (which is BigQuery but it should be hidden from consumers)"""

  def __init__(
      self,
      config: Config,
      creds: credentials.Credentials,
      target: ConfigTarget,
  ) -> None:
    self.config = config
    logger.debug('DataGateway.init: project_id=%s, bq_location=%s',
                 config.project_id,
                 target.bq_dataset_location if target else None)
    self.bq_client = bigquery.Client(
        project=config.project_id,
        credentials=creds,
        location=target.bq_dataset_location if target else None)
    self.bq_client.default_project = config.project_id
    self.bq_utils = CloudBigQueryUtils(self.bq_client)
    self.credentials = creds

  def _recreate_dataset(self,
                        dataset_id,
                        dataset_location,
                        backup_existing_tables=True):
    # Construct a BigQuery client object.
    fully_qualified_dataset_id = f'{self.config.project_id}.{dataset_id}'
    to_create = False
    if dataset_location == 'europe':
      dataset_location = 'eu'
    ds_backup = None
    try:
      ds: Dataset = self.bq_client.get_dataset(fully_qualified_dataset_id)
      if ds.location and dataset_location and \
         ds.location.lower() != dataset_location.lower() or \
         not ds.location and dataset_location or \
         ds.location and not dataset_location:
        # the current dataset's location and desired one are different,
        # we need to recreate the dataset
        logger.info(
            'Existing dataset needs to be recreated due to different locations (current: %s, needed: %s).',
            ds.location, dataset_location)
        # but before doing so let's copy everything aside
        if backup_existing_tables:
          logger.debug('Backing up existing tables in the %s', dataset_id)
          # we'll create a backup dataset in the same location as the current one
          ds_backup = bigquery.Dataset(fully_qualified_dataset_id + '_backup_' +
                                       datetime.now().strftime('%Y%m%d_%H%M'))
          ds_backup.location = ds.location
          ds_backup = self.bq_client.create_dataset(ds_backup, True)
          i = 0
          for t in self.bq_client.list_tables(ds):
            self.bq_client.copy_table(
                ds.dataset_id + '.' + t.table_id,
                ds_backup.dataset_id + '.' + t.table_id,
                location=ds.location)
            i += 1
          logger.debug('{i} tables were copied to %s', ds_backup.dataset_id)
        self.bq_client.delete_dataset(ds, True)
        logger.debug('Dataset %s deleted', ds.dataset_id)
        to_create = True
      else:
        logger.info('Dataset %s already exists in %s.',
                    fully_qualified_dataset_id, dataset_location)
    except exceptions.NotFound as e:
      logger.warning(e)
      logger.info('Dataset %s is not found in %s.', fully_qualified_dataset_id,
                  dataset_location)
      to_create = True
    if to_create:
      dataset = bigquery.Dataset(fully_qualified_dataset_id)
      dataset.location = dataset_location
      ds = self.bq_client.create_dataset(dataset)
      logger.info('Dataset %s created in %s.', fully_qualified_dataset_id,
                  dataset_location)
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
    self.bq_client = bigquery.Client(
        project=self.config.project_id,
        credentials=self.credentials,
        location=bq_dataset_location)

    self._recreate_dataset(bq_dataset_id, bq_dataset_location, True)

    table_name = f'{bq_dataset_id}.audiences'
    self._ensure_table(table_name, TableSchemas.audiences)

    table_name = f'{bq_dataset_id}.audiences_log'
    self._ensure_table(table_name, TableSchemas.audiences_log)

    # NOTE: when we change schema for test/control tables
    # we have to update them in all installations.
    # Here's the last update in schema (ttl added for control tables)
    # => 2.0 completed for all customers
    # update user segments tables for control users (adding ttl)
    # audiences = self.get_audiences(target)
    # for audience in audiences:
    #   if audience.table_name:
    #     query = f"SELECT table_name FROM {bq_dataset_id}.INFORMATION_SCHEMA.TABLES WHERE table_name like '{audience.table_name}_control_%' ORDER BY 1"
    #     rows = self.execute_query(query)
    #     for row in rows:
    #       table_name = row['table_name']
    #       self._ensure_table(
    #           f'{bq_dataset_id}.{table_name}',
    #           TableSchemas.daily_control_users,
    #           strict=False)

    # => 3.0
    # Update for v3 (user_normalized with incrementality)
    if target.ga4_loopback_recreate:
      # in case if the user is switching back from ga4_loopback_recreate=False,
      # drop all incremental tables and view
      query = f"""SELECT table_name
  FROM {target.bq_dataset_id}.INFORMATION_SCHEMA.TABLES
  WHERE table_name like '{TABLE_USERS_NORMALIZED}_%' ORDER BY 1 DESC
      """
      response = self.execute_query(query)
      tables = [r['table_name'] for r in response]
      # first drop all existing tables users_normalized_*
      for table in tables:
        query = f'DROP TABLE IF EXISTS `{target.bq_dataset_id}.{table}`'
        self.execute_query(query)
      # drop the view users_normalized if it exists
      query = f'DROP VIEW IF EXISTS {target.bq_dataset_id}.{TABLE_USERS_NORMALIZED}'
      self.execute_query(query)
    else:
      # initialization in default mode from old schema (pre v3)
      # when users_normalized was a single table
      creation_time = self.bq_utils.get_table_creation_time(
          target.bq_dataset_id, TABLE_USERS_NORMALIZED, table_only=True)
      if creation_time:
        # the table exists (not view), so we're migrating from the old schema
        # first we need to rename existing users_normalized to suffixed table
        # with date of its creation date
        table_name = f'{target.bq_dataset_id}.{TABLE_USERS_NORMALIZED}'
        suffixed_table_name = f"{TABLE_USERS_NORMALIZED}_{creation_time.strftime('%Y%m%d')}"
        query = f'ALTER TABLE `{table_name}` RENAME TO `{suffixed_table_name}`'
        self.execute_query(query)
        query = f'CREATE OR REPLACE VIEW `{table_name}` AS SELECT * FROM `{table_name}_*`'
        self.execute_query(query)
        logger.warning(
            'users_normalized renamed to %s, view users_normalized created',
            suffixed_table_name)
      # TODO: there's a problem: if the user has changed loopback_window,
      # it won't go into effect as we're not recreating the table and
      # don't know for what period it was created originally.

  def _ensure_table(self,
                    table_name,
                    expected_schema: list[bigquery.SchemaField],
                    strict=False):
    table_ref = bigquery.TableReference.from_string(table_name,
                                                    self.config.project_id)
    try:
      table = self.bq_client.get_table(table_ref)
      logger.debug('Initialize: table %s found', table_name)

      current_schema = table.schema
      added_fields: list[bigquery.SchemaField] = []
      updated_fields: list[bigquery.SchemaField] = []
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
          added_fields.append(expected_field)
        elif (expected_field_type != current_field.field_type or
              expected_field.mode != current_field.mode):
          updated_fields.append(expected_field)

      deleted_fields = []
      for current_field in current_schema:
        if not any(
            field.name == current_field.name for field in expected_schema):
          deleted_fields.append(current_field)

      if deleted_fields:
        # drop columns (update_table doesn't support removing columns)
        logger.debug('Removing excess columns for %s: %s', table_name,
                     deleted_fields)
        for field in deleted_fields:
          sql = f'ALTER TABLE `{table_name}` DROP COLUMN {field.name}'
          self.execute_query(sql)
        # refresh the table as w/o it we'll have 'PRECONDITION_FAILED: 412' error on update_table
        table = self.bq_client.get_table(table_ref)
      if added_fields:
        logger.debug('Adding new columns to %s: %s', table_name, added_fields)
        for field in added_fields:
          sql = f'ALTER TABLE `{table_name}` ADD COLUMN {field.name} {field.field_type}'
          self.execute_query(sql)
          if field.default_value_expression:
            if isinstance(field.default_value_expression, (str)):
              default_value_expression = "'" + field.default_value_expression + "'"
            else:
              default_value_expression = field.default_value_expression
            # 1. ALTER TABLE my_table ADD COLUMN field;
            # 2. ALTER TABLE my_table ALTER COLUMN field SET DEFAULT '';
            # 3. UPDATE my_table SET field = '' WHERE TRUE;
            sql = f'ALTER TABLE `{table_name}` ALTER COLUMN {field.name} SET DEFAULT {default_value_expression}'
            self.execute_query(sql)
            sql = f'UPDATE `{table_name}` SET {field.name} = {default_value_expression} WHERE TRUE'
            self.execute_query(sql)
        table = self.bq_client.get_table(table_ref)

      if updated_fields:
        table.schema = expected_schema
        try:
          logger.debug('Updating table %s with new schema:\n %s', table_name,
                       updated_fields)
          table = self.bq_client.update_table(table, ['schema'])
          logger.info('Table %s schema updated', table_name)
        except Exception as e:
          logger.error(e)
          if getattr(e, 'errors', None) and e.errors[0]:
            if e.errors[0].get('debugInfo',
                               None) and e.errors[0]['debugInfo'].startswith(
                                   '[SCHEMA_INCOMPATIBLE]'):
              logger.warning(
                  'Table %s schema is incompatible and the table has to be recreated',
                  table_name)
              backup_table_id = table_name + '_backup'
              try:
                self.bq_client.delete_table(backup_table_id, not_found_ok=True)
              except:
                logger.warning('Failed to delete backup table %s',
                               backup_table_id)
              try:
                self.bq_client.copy_table(table, backup_table_id)
              except Exception as e1:
                logger.warning(
                    'Failed to backup table before recreating %s: %s',
                    table.full_table_id, e1)
              self.bq_client.delete_table(table, not_found_ok=True)
              table = bigquery.Table(table_ref, schema=expected_schema)
              self.bq_client.create_table(table)
              return
          logger.warning(
              'The error is not a scheme_incompatible error or we failed to parse it'
          )
          raise
      else:
        logger.debug('Table %s has compatible schema', table_name)
    except exceptions.NotFound:
      logger.debug("Initialize: Creating '%s' table", table_name)
      table = bigquery.Table(table_ref, schema=expected_schema)
      self.bq_client.create_table(table)

  def execute_query(
      self,
      query: str,
      return_stat=False) -> list[dict] | tuple[list[dict], float, int]:
    """Execute a query.

      Args:
        query: a SQL query to execute
        return_stat: if true then function returns a tuple

      Returns:
        results or tuple with results, cost, number of billed bytes
    """
    ts_start = datetime.now()
    lines = [
        f'{i}: {line.rstrip()}'
        for i, line in enumerate(query.strip().split('\n'), start=1)
    ]
    query_logged = '\n'.join(lines)
    logger.debug('Executing SQL query: \n%s', query_logged)
    try:
      query_job = self.bq_client.query(query)
      results = query_job.result()
      # current pricing for on-demand model (2024): $6.25 per TiB
      cost = 6.25 * query_job.total_bytes_billed / 1024**4
    except exceptions.BadRequest as e:
      if e.errors and e.errors[0]['reason'] == 'invalidQuery' and e.errors[0][
          'message'].startswith('Unrecognized name:'):
        raise AppNotInitializedError(
            'Query failed to execute due to either mistake in the query'
            f' or schema incompatibility. {e.errors[0]["message"]}.'
            ' Please re-initialize application') from e
      raise QueryExecutionError(
          f'Query execution error: {e.errors[0]["message"] if e.errors else str(e)}',
          query) from e

    fields = [field.name for field in results.schema]
    data_list = []

    for row in results:
      row_dict = {}
      for i, value in enumerate(row):
        row_dict[fields[i]] = value
      data_list.append(row_dict)

    elapsed = datetime.now() - ts_start
    logger.debug(
        'Query executed successfully (elapsed: %s, cost: %.4f, total bytes billed: %s)',
        format_duration(elapsed), cost, query_job.total_bytes_billed)
    if return_stat:
      return data_list, cost, query_job.total_bytes_billed

    return data_list

  def get_ga4_table_name(self, target: ConfigTarget, wildcard=False):
    ga_fqn = f'{target.ga4_project}.{target.ga4_dataset}.{target.ga4_table}'
    if wildcard and ga_fqn[-2:] != '_*':
      ga_fqn += '_*'
    return ga_fqn

  def check_ga4(self, ga4_project: str, ga4_dataset: str, ga4_table='events'):
    query = f"SELECT table_name FROM `{ga4_project}.{ga4_dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name LIKE '{ga4_table}_%' ORDER BY 1 DESC"
    try:
      # TODO: if the target config has't been configured then we don't have a BQ
      # location so we don't know where execute the query,
      # by default it'll use US location
      response = self.execute_query(query)
      tables = [r['table_name'] for r in response]
    except BaseException as e:
      logger.error(e)
      sa = f"{self.config.project_id}@appspot.gserviceaccount.com"
      raise Exception(
          f"Incorrect GA4 table name or the application's service account ({sa})"
          " doesn't have access permissions to the BigQuery dataset.\n"
          f"Original error: {e}") from e

    if logger.isEnabledFor(logger.level):
      logger.debug('Found GA4 events tables: %s', tables)
    yesterday = (date.today() - timedelta(days=2)).strftime('%Y%m%d')
    # first row should be 'events_intraday_yyymmdd' (for today), and previous one 'events_yyyymmdd' for tomorrow
    found = next((t for t in tables if t == ga4_table + '_' + yesterday), None)
    if not found:
      raise Exception(
          f"The speficied GA4 dataset ({ga4_project}.{ga4_dataset}) does exists"
          " but does not seem to be updated as we couldn't find an events table"
          f" for the day before yesterday ('{ga4_table + '_' + yesterday}')")

    return tables

  def get_ga4_stats(self, target: ConfigTarget, days_ago_start: int,
                    days_ago_end: int):
    if days_ago_start < days_ago_end:
      raise ValueError('days_ago_start should be greater than days_ago_end')

    ga_table = self.get_ga4_table_name(target, True)
    logger.debug('Loading GA4 stats for target %s (GA4 table %s)', target.name,
                 ga_table)
    # load events stats
    query = f"""
SELECT
  app_info.id AS app_id,
  event_name as event,
  count(1) as count
FROM
  `{ga_table}`
WHERE
    app_info.id IS NOT NULL
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
    logger.debug('Loaded event stats per app_id: %s', len(events_stat))
    app_ids = set()
    events_stat_dict = {}
    for app_id, group in groupby(
        sorted(events_stat, key=lambda x: x['app_id']),
        key=lambda x: x['app_id']):
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
    app_info.id IS NOT NULL
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
    logger.debug('Loaded country stats per app_id: %s', len(countries_stat))

    countries_stat_dict = {}
    ts_start = datetime.now()
    for app_id, group in groupby(
        sorted(countries_stat, key=lambda x: x['app_id']),
        key=lambda x: x['app_id']):
      countries = list(group)
      for country in countries:
        country_name = country['country']
        code = country_name_to_code_cache.get(country_name, None)
        if not code:
          code = coco.convert(
              names=[country['country']], to='ISO2', not_found=None)
          country_name_to_code_cache[country_name] = code
        country['country_code'] = code
        if code == country['country']:
          logger.warning('Could not find country by its name %s',
                         country['country'])
      countries_stat_dict[app_id] = countries
    elapsed = datetime.now() - ts_start
    logger.debug('Enriched stats by country with country codes (elapsed %s)',
                 elapsed)

    return {
        'app_ids': sorted(list(app_ids)),
        'events': events_stat_dict,
        'countries': countries_stat_dict
    }

  def get_audiences(self,
                    target: ConfigTarget,
                    audience_name: str = None) -> list[Audience]:
    condition = f"WHERE name='{audience_name}'" if audience_name else ''
    query = f"""
SELECT
  name, app_id, table_name, countries, events_include, events_exclude, days_ago_start, days_ago_end, user_list, created, mode, query, ttl, split_ratio
FROM `{target.bq_dataset_id}.audiences`
{condition}
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
      audience.split_ratio = row['split_ratio']
      audiences.append(audience)
    return audiences

  def update_audiences(self, target: ConfigTarget, audiences: list[Audience]):
    table_name = f'{target.bq_dataset_id}.audiences'
    audiences = audiences or []
    audiences_old = self.get_audiences(target)
    logger.debug('Current audiences:')
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
      query_params.append(
          bigquery.ScalarQueryParameter('query' + str(idx), 'STRING', i.query))
      selects.append(f"""SELECT '{i.name}' name, '{i.app_id}' app_id,
  {"'" + i.table_name + "'"} table_name,
  {i.countries} countries,
  {i.events_include} events_include, {i.events_exclude} events_exclude,
  {i.days_ago_start} days_ago_start, {i.days_ago_end} days_ago_end,
  {"'" + i.user_list + "'" if i.user_list is not None else "CAST(NULL as STRING)"} user_list,
  {"'" + i.mode + "'"} mode,
  @query{idx} query,
  {i.ttl} ttl,
  {"NULL" if not i.split_ratio else i.split_ratio} split_ratio
""")
    sql_selects = '\nUNION ALL\n'.join(selects)
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
    t.ttl = s.ttl,
    t.split_ratio = s.split_ratio
WHEN NOT MATCHED THEN
  INSERT (name, app_id, table_name, countries, events_include, events_exclude, days_ago_start, days_ago_end, created, mode, query, ttl, split_ratio)
  VALUES (s.name, s.app_id, s.table_name, s.countries, s.events_include, s.events_exclude, s.days_ago_start, s.days_ago_end, CURRENT_TIMESTAMP(), s.mode, s.query, s.ttl, s.split_ratio)
"""
    logger.debug(query)
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    self.bq_client.query(query, job_config=job_config).result()

    # delete removed audiences
    sql_names_to_delete = ','.join(
        ["'" + item.name + "'" for item in to_remove])
    if sql_names_to_delete:
      query = f'DELETE FROM `{table_name}` WHERE name in ({sql_names_to_delete})'
      logger.debug(query)
      self.bq_client.query(query).result()

    names_to_delete = [item.name for item in to_remove]
    result = {'deleted': names_to_delete}

    for audience in to_remove:
      self._on_audience_removed(target, audience)
    return result

  def _on_audience_removed(self, target: ConfigTarget, audience: Audience):
    table_name = target.bq_dataset_id + '.' + audience.table_name
    self.bq_client.delete_table(table_name, not_found_ok=True)
    for suffix in ['all', 'test', 'control', 'uploaded']:
      self._delete_audience_tables(target, audience.table_name, suffix)

  def _delete_audience_tables(self, target: ConfigTarget, table_name: str,
                              suffix: str):
    meta_table_name = target.bq_dataset_id + '.INFORMATION_SCHEMA.TABLES'
    query = f"SELECT table_name FROM {meta_table_name} WHERE table_name like '{table_name}_{suffix}_%' ORDER BY 1"
    rows = self.execute_query(query)
    for row in rows:
      table_name = target.bq_dataset_id + '.' + row['table_name']
      self.bq_client.delete_table(table_name, not_found_ok=True)

  def _read_file(self, filename):
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    filename = os.path.join(script_dir, filename)
    with open(filename) as f:
      query = f.read()
    return query

  def get_audience_sampling_query(self, target: ConfigTarget,
                                  audience: Audience):
    days_ago_start = int(audience.days_ago_start)
    days_ago_end = int(audience.days_ago_end)
    day_start = (datetime.now() -
                 timedelta(days=days_ago_start)).strftime('%Y%m%d')
    day_end = (datetime.now() - timedelta(days=days_ago_end)).strftime('%Y%m%d')
    logger.debug('Creating user segment for time window: %s - %s', day_start,
                 day_end)

    countries = ','.join([f"'{c}'" for c in audience.countries])
    events_include = audience.events_include or []
    events_exclude = audience.events_exclude or []
    # if not 'first_open' in events_include:
    #   events_include = ['first_open'] + events_include
    if not 'app_remove' in events_exclude:
      events_exclude = ['app_remove'] + events_exclude

    all_events = events_include + events_exclude
    if not 'session_start' in all_events:
      all_events = ['session_start'] + all_events

    all_events_list = ', '.join([f"'{event}'" for event in all_events])
    search_condition = ''
    if events_include:
      search_condition += ' AND '.join(
          [f"'{event}' IN UNNEST(events)" for event in events_include])
    if search_condition:
      search_condition += ' AND '
    if events_exclude:
      search_condition += ' AND '.join(
          [f"'{event}' NOT IN UNNEST(events)" for event in events_exclude])

    if audience.query:
      query = audience.query
      logger.debug('Using customer audience query:\n%s', query)
    else:
      query = self._read_file('prepare.sql')

    try:
      query = query.format(
          **{
              'source_table':
                  self.get_ga4_table_name(target, True),
              'day_start':
                  day_start,
              'day_end':
                  day_end,
              'app_id':
                  audience.app_id,
              'countries_clause':
                  f'f.country IN ({countries}) ' if audience
                  .countries else 'TRUE',
              'countries':
                  countries,
              'all_users_table':
                  target.bq_dataset_id + '.' + TABLE_USERS_NORMALIZED,
              'all_events_list':
                  all_events_list,
              'SEARCH_CONDITIONS':
                  search_condition,
              'dataset':
                  target.bq_dataset_id,
          })
    except KeyError as e:
      raise Exception(
          f'An error occured during substituting macros into audience query, unknown macro {e} was used'
      ) from e

    return query

  def get_base_conversion_query(self,
                                target: ConfigTarget,
                                audience: Audience,
                                conversion_window_days: int,
                                date_start: date = None,
                                date_end: date = None):
    days_ago_start = int(audience.days_ago_start)
    days_ago_end = int(audience.days_ago_end)
    audience_duration = abs(days_ago_end - days_ago_start)
    if date_start and date_end:
      # date_start AND date_end: we might need to adjust one of them (start or end)
      delta = audience_duration + conversion_window_days
      if (date_end -
          date_start).days < audience_duration + conversion_window_days:
        if date_start + timedelta(days=delta) < date.today():
          date_end = date_start + timedelta(days=delta)
        else:
          date_start = date_end - timedelta(days=delta)
    else:
      if not date_start:
        delta = max(30, audience_duration + conversion_window_days)
        date_start = (date_end if date_end else date.today()) - timedelta(
            days=delta)
      if not date_end:
        date_end = date.today()

    date_audience_start = date_start
    date_audience_end = date_start + timedelta(days=audience_duration)
    date_conversion_start = date_start + timedelta(days=audience_duration + 1)
    date_conversion_end = date_start + timedelta(days=audience_duration +
                                                 conversion_window_days)

    countries = ','.join([f"'{c}'" for c in audience.countries])
    conversion_events = [
        item for item in audience.events_exclude if item != 'app_remove'
    ]
    conversion_events_list = ', '.join(
        [f"'{event}'" for event in conversion_events])
    events_include = audience.events_include or []
    events_exclude = audience.events_exclude or []
    if not 'app_remove' in events_exclude:
      events_exclude = ['app_remove'] + events_exclude
    all_events = events_include + events_exclude
    all_events_list = ', '.join([f"'{event}'" for event in all_events])
    audience_events_list = ', '.join([f"'{event}'" for event in events_include])

    query = self._read_file('base_conversion.sql')

    try:
      query = query.format(
          **{
              'source_table':
                  self.get_ga4_table_name(target, True),
              'all_users_table':
                  target.bq_dataset_id + '.' + TABLE_USERS_NORMALIZED,
              'date_start':
                  date_start.strftime('%Y%m%d'),
              'date_end':
                  date_end.strftime('%Y%m%d'),
              'app_id':
                  audience.app_id,
              'countries':
                  countries,
              'countries_clause':
                  f'country IN ({countries}) ' if audience
                  .countries else 'TRUE',
              'all_events_list':
                  all_events_list,
              'audience_events_list':
                  audience_events_list,
              'date_audience_start':
                  date_audience_start.strftime('%Y%m%d'),
              'date_audience_end':
                  date_audience_end.strftime('%Y%m%d'),
              'conversion_events_list':
                  conversion_events_list,
              'date_conversion_start':
                  date_conversion_start.strftime('%Y%m%d'),
              'date_conversion_end':
                  date_conversion_end.strftime('%Y%m%d')
          })
    except KeyError as e:
      raise Exception(
          f'An error occured during substituting macros into audience query, unknown macro {e} was used'
      ) from e
    return query, date_start, date_end

  def get_base_conversion(self,
                          target: ConfigTarget,
                          audience: Audience,
                          conversion_window_days: int,
                          date_start: date = None,
                          date_end: date = None):
    if not conversion_window_days:
      conversion_window_days = 7
    query, date_start, date_end = self.get_base_conversion_query(
        target, audience, conversion_window_days, date_start, date_end)
    row = self.execute_query(query)[0]
    logger.info(row)
    return {
        'audience': row['audience'],
        'converted': row['converted'],
        'cr': float(row['cr']) if row['cr'] else None,
        'query': query,
        'conversion_window_days': conversion_window_days,
        'date_start': date_start.strftime('%Y%m%d'),
        'date_end': date_end.strftime('%Y%m%d'),
    }

  def _parse_duration(self, duration: str) -> tuple[int, int, int]:
    years = months = days = 0
    matches = re.findall(r'(\d+[YMD])', duration.upper())
    if not matches:
      raise ValueError(f'Unknown duration format: {duration}')
    for match in matches:
      if 'Y' in match:
        years = int(match.replace('Y', ''))
      elif 'M' in match:
        months = int(match.replace('M', ''))
      elif 'D' in match:
        days = int(match.replace('D', ''))
      else:
        raise ValueError(f'Unknown duration format: {duration}')
    return years, months, days

  def _create_users_normalized_table_backfill(self,
                                              target: ConfigTarget,
                                              incremental: bool = True):
    if target.ga4_loopback_window:
      years, months, days = self._parse_duration(target.ga4_loopback_window)
      start_date = date.today()
      if years > 0:
        start_date = start_date - relativedelta(years=years)
      if months > 0:
        start_date = start_date - relativedelta(months=months)
      if days > 0:
        start_date = start_date - timedelta(days=days)
      if start_date == date.today():
        raise ValueError(
            'ga4_loopback_window does not define any time range: ' +
            target.ga4_loopback_window)
    else:
      start_date = date.today() - relativedelta(years=1)
    start_day = start_date.strftime('%Y%m%d')
    end_day = date.today().strftime('%Y%m%d')
    self._create_users_normalized_table(target, start_day, end_day, incremental)

  def _create_users_normalized_table(self,
                                     target: ConfigTarget,
                                     start_day: str,
                                     end_day: str,
                                     incremental: bool = True):
    query = self._read_file('prepare_users.sql')
    if not start_day or not end_day:
      raise ValueError(
          'Creating users_normalized: start_date and end_date must be specified'
      )
    if start_day == end_day:
      range_condition = f"AND _TABLE_SUFFIX = '{start_day}'"
    else:
      range_condition = f"AND _TABLE_SUFFIX BETWEEN '{start_day}' AND '{end_day}'"
    query = query.format(
        **{
            'source_table': self.get_ga4_table_name(target, True),
            'SEARCH_CONDITIONS': range_condition
        })
    if incremental:
      destination_table_base = f'{target.bq_dataset_id}.{TABLE_USERS_NORMALIZED}'
      destination_table = f'{destination_table_base}_{end_day}'
      query = f'CREATE OR REPLACE TABLE `{destination_table}` AS\n' + query
      self.execute_query(query)
      query = f'CREATE OR REPLACE VIEW `{destination_table_base}` AS SELECT * FROM `{destination_table_base}_*`'
      self.execute_query(query)
    else:
      # just one table 'users_normalized' with all data
      query = f'CREATE OR REPLACE TABLE `{destination_table_base}` AS\n' + query
      self.execute_query(query)

  def ensure_users_normalized(self, target: ConfigTarget, today=date.today()):
    if target.ga4_loopback_recreate:
      # users_normalized is non-incremental
      # check its creation date and if it's not today
      # recreate it for loopback_window time range
      creation_time = self.bq_utils.get_table_creation_time(
          target.bq_dataset_id, TABLE_USERS_NORMALIZED, table_only=True)
      if creation_time:
        logger.info('user_normalized table exists, creation time: %s',
                    creation_time)
        if creation_time.date() != datetime.now(timezone.utc).date():
          # table was created not today
          logger.debug('Table user_normalized needs to be recreated')
          self._create_users_normalized_table_backfill(
              target, incremental=False)
    else:
      # users_normalized is incremental
      query = f"""SELECT table_name
  FROM {target.bq_dataset_id}.INFORMATION_SCHEMA.TABLES
  WHERE table_name like '{TABLE_USERS_NORMALIZED}_%' ORDER BY 1 DESC
      """
      response = self.execute_query(query)
      tables = [r['table_name'] for r in response]

      if not tables:
        logger.info('Creating users_normalized table for the first time')
        self._create_users_normalized_table_backfill(target)
      else:
        # detect the last day till which we have data in users_normalized table
        last_day = [t.split('_')[-1] for t in tables][0]
        # now we need to create a table users_normalized_{today} that includes
        # events starting the day after last_day till today
        last_day = datetime.strptime(last_day, '%Y%m%d').date()
        if last_day == today:
          # today's table already exists, no need to do anything
          return

        start_day = (last_day + timedelta(days=1)).strftime('%Y%m%d')
        end_day = today.strftime('%Y%m%d')
        self._create_users_normalized_table(target, start_day, end_day)

  def sample_audience_users(self,
                            target: ConfigTarget,
                            audience: Audience,
                            suffix: str | None  = None,
                            return_only_new_users=False):
    """Segment an audience - takes an audience description and fetches the users
       from GA4 events according to the conditions.

       Returns:
        DataFrame with columns user, brand, osv, days_since_install, src, n_sessions
    """
    suffix = datetime.now().strftime('%Y%m%d') if suffix is None else suffix
    audience.ensure_table_name()
    destination_table = self._get_user_segment_table_full_name(
        target, audience.table_name, 'all', suffix)
    query = self.get_audience_sampling_query(target, audience)
    query = f'CREATE OR REPLACE TABLE `{destination_table}` AS\n' + query

    self.execute_query(query)

    if return_only_new_users:
      # test/control tables can not yet exist (on the first day of sampling),
      # so if it's the case we're switching off return_only_new_users flag to
      # prevent feching from them in load_sampled_users
      query = f"SELECT table_name FROM {target.bq_dataset_id}.INFORMATION_SCHEMA.TABLES WHERE table_name like '{audience.table_name}_test_%' LIMIT 1"
      rows = self.execute_query(query)
      if not rows:
        return_only_new_users = False
      else:
        query = f"SELECT table_name FROM {target.bq_dataset_id}.INFORMATION_SCHEMA.TABLES WHERE table_name like '{audience.table_name}_control_%' LIMIT 1"
        rows = self.execute_query(query)
        if not rows:
          return_only_new_users = False

    df = self.load_sampled_users(
        target, audience, suffix, only_new_users=return_only_new_users)
    return df

  def load_sampled_users(self,
                         target: ConfigTarget,
                         audience: Audience,
                         suffix: str | None = None,
                         only_new_users=False):
    """Load users of a segment (sampled users of one day).
       Can be either all users, or only new users.
    """
    suffix = datetime.now().strftime('%Y%m%d') if suffix is None else suffix
    audience_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'all', suffix)
    query = f"""
SELECT
  user,
  brand,
  osv,
  days_since_install,
  src,
  n_sessions
FROM `{audience_table_name}` a
"""
    if only_new_users:
      query += f"""WHERE
  NOT EXISTS (SELECT user FROM `{self._get_user_segment_table_full_name(target, audience.table_name, 'control', '*')}` t WHERE t.user=a.user AND _TABLE_SUFFIX < '{suffix}') AND
  NOT EXISTS (SELECT user FROM `{self._get_user_segment_table_full_name(target, audience.table_name, 'test', '*')}` t WHERE t.user=a.user AND _TABLE_SUFFIX < '{suffix}')
      """

    logger.debug('Executing SQL query: %s', query)
    df = pd.read_gbq(
        query=query,
        project_id=self.config.project_id,
        credentials=self.credentials)
    return df

  def _get_user_segment_tables(self,
                               target: ConfigTarget,
                               audience_table_name,
                               group_name: Literal['test']
                               | Literal['control'] = 'test',
                               suffix: str | None = None,
                               include_dataset=False):
    bq_dataset_id = target.bq_dataset_id
    query = f"SELECT table_name FROM {bq_dataset_id}.INFORMATION_SCHEMA.TABLES WHERE table_name LIKE '{audience_table_name}_{group_name}_%' ORDER BY 1 DESC"
    rows = self.execute_query(query)
    tables = [row['table_name'] for row in rows]
    if include_dataset:
      return [f'{bq_dataset_id}.{t}' for t in tables]
    return tables

  def _get_user_segment_table_full_name(self,
                                        target: ConfigTarget,
                                        audience_table_name,
                                        group_name: Literal['test', 'control'],
                                        suffix: str | None = None):
    bq_dataset_id = target.bq_dataset_id
    suffix = datetime.now().strftime('%Y%m%d') if suffix is None else suffix
    test_table_name = f'{bq_dataset_id}.{audience_table_name}_{group_name}_{suffix}'
    return test_table_name

  def save_sampled_users(self,
                         target: ConfigTarget,
                         audience: Audience,
                         users_test: pd.DataFrame,
                         users_control: pd.DataFrame,
                         suffix: str | None = None):
    """Save sampled users from two DataFrames into two new tables (_test and _control)"""
    project_id = self.config.project_id
    test_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'test', suffix)
    if len(users_test) == 0:
      self._ensure_table(test_table_name, TableSchemas.daily_test_users)
    else:
      # add 'status' column (empty for all rows)
      users_test = users_test.assign(status=None).astype({'status': 'Int64'})
      # add 'ttl' column with audience's initial ttl
      users_test = users_test.assign(ttl=audience.ttl).astype({'ttl': 'Int64'})
      pandas_gbq.to_gbq(
          users_test[['user', 'status', 'ttl']],
          test_table_name,
          project_id,
          if_exists='replace')

    control_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'control', suffix)
    if len(users_control) == 0:
      self._ensure_table(control_table_name, TableSchemas.daily_control_users)
    else:
      # add 'ttl' column with audience's initial ttl
      users_control = users_control.assign(ttl=audience.ttl).astype(
          {'ttl': 'Int64'})
      pandas_gbq.to_gbq(
          users_control[['user', 'ttl']],
          control_table_name,
          project_id,
          if_exists='replace')

    logger.info(
        'Sampled users for audience %s saved to %s (%s rows)/%s (%s rows) tables',
        audience.name, test_table_name, len(users_test), control_table_name,
        len(users_control))

  def add_previous_sampled_users(self,
                                 target: ConfigTarget,
                                 audience: Audience,
                                 suffix: str | None = None):
    """Add users from previous days into today's segment.

      It implements 'user affinity'. We take only users sampled today but
      their affinity to group (test or control) should not change.
    """
    suffix = datetime.now().strftime('%Y%m%d') if suffix is None else suffix
    segment_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'all', suffix)
    # test:
    group_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'test', suffix)
    group_prev_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'test', '*')
    ttl = audience.ttl
    query = f"""
  INSERT INTO `{group_table_name}` (user, ttl)
  SELECT user, {ttl} FROM `{segment_table_name}` t1
  WHERE
    EXISTS (SELECT user FROM `{group_prev_table_name}` t WHERE t.user=t1.user AND _TABLE_SUFFIX < '{suffix}')
  """
    self.execute_query(query)
    # control:
    group_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'control', suffix)
    group_prev_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'control', '*')
    query = f"""
  INSERT INTO `{group_table_name}` (user, ttl)
  SELECT user, {ttl} FROM `{segment_table_name}` t1
  WHERE
    EXISTS (SELECT user FROM `{group_prev_table_name}` t WHERE t.user=t1.user AND _TABLE_SUFFIX < '{suffix}')
  """
    self.execute_query(query)

  def _copy_users_from_previous_day(self, table_name, table_name_yesterday):
    logger.debug('Adding users with TTL>1 from yesterday (%s)',
                 table_name_yesterday)
    query = f"""INSERT INTO `{table_name}` (user, ttl)
  SELECT user, ttl-1 FROM `{table_name_yesterday}` t1
  WHERE
    NOT EXISTS (SELECT user FROM `{table_name}` WHERE user=t1.user)
    AND ttl>1
  """
    self.execute_query(query)
    logger.debug('Added test users from previous day with TTL>1')

  def add_yesterdays_users(self,
                           target: ConfigTarget,
                           audience: Audience,
                           suffix: str | None = None):
    """Add test users from yesterday with TTL>1 into today's test users."""
    if audience.ttl > 1:
      test_table_name = self._get_user_segment_table_full_name(
          target, audience.table_name, 'test', suffix)
      control_table_name = self._get_user_segment_table_full_name(
          target, audience.table_name, 'control', suffix)
      test_tables = self._get_user_segment_tables(
          target, audience.table_name, 'test', suffix, include_dataset=True)
      control_tables = self._get_user_segment_tables(
          target, audience.table_name, 'control', suffix, include_dataset=True)
      if test_tables:
        try:
          idx = test_tables.index(test_table_name)
          if idx != len(test_tables) - 1:
            test_table_name_yesterday = test_tables[idx + 1]
            self._copy_users_from_previous_day(test_table_name,
                                               test_table_name_yesterday)
            # NOTE: if a previous test table exists then there should be a control one
            control_table_name_yesterday = control_tables[idx + 1]
            self._copy_users_from_previous_day(control_table_name,
                                               control_table_name_yesterday)
        except ValueError:
          pass
    return None

  def load_audience_segment(self,
                            target: ConfigTarget,
                            audience: Audience,
                            group_name: Literal['test']
                            | Literal['control'] = 'test',
                            suffix: str | None = None) -> list[str]:
    """Loads test users of a given audience for a particular segment (by default - today)"""
    table_name = self._get_user_segment_table_full_name(target,
                                                        audience.table_name,
                                                        group_name, suffix)
    query = f"""SELECT user FROM `{table_name}`"""
    try:
      rows = self.execute_query(query)
      users = [row['user'] for row in rows]
    except exceptions.NotFound:
      logger.debug("Table '%s' not found (audience segment is empty)",
                   table_name)
      users = []
    return users

  def update_audience_segment_status(self, target: ConfigTarget,
                                     audience: Audience, suffix: str,
                                     failed_users: list[str]):
    """Update users statuses in a segment table (by default - for today)
        Returns:
          tuple (new_user_count, test_user_count, control_user_count)
    """
    # Originally all users in the segment table ({audience_table_name}_test_yyymmdd) have status=NULL
    # We'll update the column to 1 for all except ones in the failed_users list
    table_name = self._get_user_segment_table_full_name(target,
                                                        audience.table_name,
                                                        'test', suffix)
    if not failed_users:
      query = f"""UPDATE `{table_name}` SET status = 1 WHERE true"""
      self.execute_query(query)
    else:
      # NOTE: create a table for failed users, but its name shoud not be one that will be caught by wildcard mask {name}_test_*
      table_name_failed = self._get_user_segment_table_full_name(
          target, audience.table_name, 'testfailed', suffix)
      schema = [bigquery.SchemaField('user', 'STRING', mode='REQUIRED')]
      table_ref = bigquery.TableReference.from_string(table_name_failed,
                                                      self.config.project_id)
      table = bigquery.Table(table_ref, schema=schema)
      table = self.bq_client.create_table(table, exists_ok=True)
      rows_to_insert = [{'user': user_id} for user_id in failed_users]
      try:
        self.bq_client.insert_rows(table, rows_to_insert)
      except BaseException as e:
        logger.error(
            'An error occurred while inserting failed user ids into %s table: %s',
            table_name_failed, e)
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

  def load_user_segment_stat(self, target: ConfigTarget, audience: Audience,
                             suffix: str):
    # load test and control user counts
    test_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'test', suffix)
    query = f'SELECT COUNT(1) as count FROM `{test_table_name}` WHERE status = 1'
    try:
      test_user_count = self.execute_query(query)[0]['count']
    except exceptions.NotFound:
      logger.info(
          "Table '%s' does not exist, skipping loading a user segment for %s",
          test_table_name, suffix)
      return 0, 0, 0, 0
    control_table_name = self._get_user_segment_table_full_name(
        target, audience.table_name, 'control', suffix)
    query = f'SELECT COUNT(1) as count FROM `{control_table_name}`'
    control_user_count = self.execute_query(query)[0]['count']

    # load new user count
    table_name_prev = self._get_user_segment_table_full_name(
        target, audience.table_name, 'test', '*')
    suffix = datetime.now().strftime('%Y%m%d') if suffix is None else suffix
    # we're fetching the number of unique users in all test tables with date suffix below the current
    # NOTE: actually the fact that a user is in a test table doesn't automatically means it was uploaded to
    query = f"""SELECT count(DISTINCT t.user) as user_count FROM `{test_table_name}` t
WHERE status = 1 AND NOT EXISTS (
  SELECT * FROM `{table_name_prev}` t0
  WHERE t0._TABLE_SUFFIX < '{suffix}' AND t.user = t0.user AND t0.status = 1
)
    """
    res = self.execute_query(query)
    new_test_user_count = res[0]['user_count']

    control_table_name_prev = self._get_user_segment_table_full_name(
        target, audience.table_name, 'control', '*')
    query = f"""SELECT count(DISTINCT t.user) as user_count FROM `{control_table_name}` t
WHERE NOT EXISTS (
  SELECT * FROM `{control_table_name_prev}` t0
  WHERE t0._TABLE_SUFFIX < '{suffix}' AND t.user = t0.user
)
    """
    res = self.execute_query(query)
    new_control_user_count = res[0]['user_count']
    return test_user_count, control_user_count, new_test_user_count, new_control_user_count

  def update_audiences_log(self, target: ConfigTarget, logs: list[AudienceLog]):
    table_name = f'{target.bq_dataset_id}.audiences_log'
    custom_retry = retry.Retry(
        timeout=60, predicate=retry.if_exception_type(exceptions.NotFound))
    table = self.bq_client.get_table(table_name, retry=custom_retry)
    rows = [{
        'name': i.name,
        'date': i.date if i.date else datetime.now(),
        'job': i.job_resource_name,
        'user_count': i.uploaded_user_count,
        'new_user_count': i.new_test_user_count,
        'new_control_user_count': i.new_control_user_count,
        'test_user_count': i.test_user_count,
        'control_user_count': i.control_user_count,
        'total_user_count': i.total_test_user_count,
        'total_control_user_count': i.total_control_user_count
    } for i in logs]
    res = self.bq_client.insert_rows(table, rows)
    if res and res[0] and res[0].get('errors', None):
      msg = res[0].get('errors', None)[0].get('message', None)
      if msg:
        raise ValueError(f'Audience log entries failed to save: {msg}')

    logger.debug('Saved audience_log: %s', rows)

  def get_audiences_log(
      self,
      target: ConfigTarget,
      *,
      include_duplicates=False) -> dict[str, list[AudienceLog]]:
    table_name = f'{target.bq_dataset_id}.audiences_log'
    query = f"""SELECT * FROM
(
  SELECT
    name,
    date,
    job,
    user_count,
    new_user_count,
    new_control_user_count,
    test_user_count,
    control_user_count,
    total_user_count,
    total_control_user_count,
    ROW_NUMBER() OVER (
      PARTITION BY name, format_date('%Y%m%d', date)
      ORDER BY name, date DESC
    ) row_number
  FROM `{table_name}`
) t
{"WHERE t.row_number = 1" if not include_duplicates else ""}
ORDER BY name, date
    """
    rows = self.execute_query(query)

    result = {}
    for name, group in groupby(rows, key=lambda x: x['name']):
      log_items = []
      for item in group:
        log_item = AudienceLog(
            name=item['name'],
            date=item['date'],
            job_resource_name=item['job'],
            uploaded_user_count=item['user_count'],
            new_test_user_count=item['new_user_count'],
            new_control_user_count=item['new_control_user_count'],
            test_user_count=item['test_user_count'],
            control_user_count=item['control_user_count'],
            total_test_user_count=item['total_user_count'],
            total_control_user_count=item['total_control_user_count'],
            failed_user_count=item['test_user_count'] - item['user_count'])
        log_items.append(log_item)
      result[name] = log_items
    return result

  def get_user_conversions_query(
      self,
      target: ConfigTarget,
      audience: Audience,
      strategy: Literal['bounded', 'unbounded'],
      date_start: date | None = None,
      date_end: date | None = None,
      country: list[str] | None = None,
      events: list[str] | None = None) -> tuple[str, date, date]:
    """Return a query for calculating conversions.

      Args:
        target: A target.
        audience: An audience description.
        strategy: A strategy to calculate conversions, 'unbounded' means taking
          events for a user only for periods when they are in a list, while
          'unbounded' takes events starting a day a user got into a list.
        date_start: Optional start date of a period.
        date_end: Optional end date of a period.
        country: Optional list of countries to additionally filter results.
        events: Optional list of conversion events to use instead of
          the audience's event.

      Returns:
        a tuple (query, date_start, date_end) where query is a SQL query,
        date_start and date_end are dates for a period, if not specified
        they will be detected as first and last day of audience import.
    """
    if date_start is None:
      log = self.get_audiences_log(target)
      log_rows = log.get(audience.name, None)
      if log_rows is None:
        # no imports for the audience, then we'll use the audience creation date
        date_start = audience.created
      else:
        # Start listing conversions makes sense from the day when first segment
        # was uploaded to Google Ads
        date_start = min(log_rows, key=lambda i: i.date).date
    if date_end is None:
      # take last day of audience_log
      rows = self.execute_query(
          f"""SELECT DATE(date) as day FROM `{target.bq_dataset_id}.audiences_log`
        WHERE NAME = '{audience.name}'
        ORDER BY date DESC LIMIT 1""")
      if rows:
        date_end = rows[0].get('day', None)
        logger.info('Detected date_end from audience_log (as last upload): %s',
                    date_end)
      if not date_end:
        date_end = date.today() - timedelta(days=1)

    if events:
      conversion_events = events
    else:
      # NOTE: all events that we ignored for sampling now are our conversions
      # (but except "app_remove").
      # TODO: if events_exclude has more than one item,
      # we need to make sure all of them happened not just one of them!
      conversion_events = [
          item for item in audience.events_exclude if item != 'app_remove'
      ]
    events_list = ', '.join([f"'{event}'" for event in conversion_events
                            ]) if conversion_events else ''
    if not events_list:
      raise ValueError(
          "Conversions cannot be calculated as audience's conversion events "
          '(excluded_event or explicitly) were not specified')
    ga_table = self.get_ga4_table_name(target, True)
    user_table = target.bq_dataset_id + '.' + audience.table_name
    if country:
      country_list = ','.join([f"'{c}'" for c in country])
      conversions_conditions = f'AND country IN ({country_list})'
      query_TotalCounts = self._read_file(
          'results_parts_TotalCounts_bycountry.sql')
    else:
      conversions_conditions = ''
      query_TotalCounts = self._read_file('results_parts_TotalCounts_all.sql')

    if strategy == 'bounded':
      query = self._read_file('results.sql')
    else:
      query = self._read_file('results_unbounded.sql')

    class Default(dict):
      """Special dict used for `str.format` to tolerate missing args."""

      def __missing__(self, key):
        return '{' + key + '}'

    query = query.format_map(Default(TotalCounts=query_TotalCounts))
    query = query.format(
        **{
            'source_table':
                ga_table,
            'events':
                events_list,
            'day_start':
                date_start.strftime('%Y%m%d'),
            'day_end':
                date_end.strftime('%Y%m%d'),
            'all_users_table':
                target.bq_dataset_id + '.' + TABLE_USERS_NORMALIZED,
            'SEARCH_CONDITIONS':
                conversions_conditions,
            'app_id':
                audience.app_id,
            'test_users_table':
                user_table + '_test_*',
            'control_users_table':
                user_table + '_control_*',
            'date_start':
                date_start.strftime('%Y-%m-%d'),
            'date_end':
                date_end.strftime('%Y-%m-%d'),
            'audiences_log':
                target.bq_dataset_id + '.audiences_log',
            'audience_name':
                audience.name,
            # forward-looking conversion window for unbounded strategy
            'conv_window':
                14  # TODO: take the window from args and/or config
        })
    return query, date_start, date_end

  def get_user_conversions(
      self,
      target: ConfigTarget,
      audience: Audience,
      strategy: Literal['bounded', 'unbounded'],
      date_start: date | None = None,
      date_end: date | None = None,
      country: list[str] | None = None,
      events: list[str] | None = None) -> tuple[list[dict], date, date]:
    """Calculate conversions.

      Args:
        target: A target.
        audience: An audience description.
        strategy: A strategy to calculate conversions, 'unbounded' means taking
          events for a user only for periods when they are in a list, while
          'unbounded' takes events starting a day a user got into a list.
        date_start: Optional start date of a period.
        date_end: Optional end date of a period.
        country: Optional list of countries to additionally filter results.
        events: Optional list of conversion events to use instead of
          the audience's event.

      Returns:
        a tuple (results, date_start, date_end) where results is a list of rows
        which is a result of executing a query, each row is a dict with columns:
        date, cum_test_regs, cum_control_regs, total_user_count,
        total_control_user_count.
        date_start and date_end are dates for a period, if not specified
        they will be detected as first and last day of audience import.
    """
    query, date_start, date_end = self.get_user_conversions_query(
        target, audience, strategy, date_start, date_end, country, events)
    result = self.execute_query(query)
    # expect columns:
    # date, cum_test_regs, cum_control_regs,
    # total_user_count, total_control_user_count
    return result, date_start, date_end

  def rebuilt_audiences_log(
      self, target: ConfigTarget,
      audience_name: str | None) -> dict[str, list[AudienceLog]]:
    audiences = self.get_audiences(target)
    audiences_log = self.get_audiences_log(target, include_duplicates=True)

    table_fq_name = f'{target.bq_dataset_id}.audiences_log'
    # 'DELETE FROM' usually fail with error:
    #   "UPDATE or DELETE statement over table 'table_name' would affect rows
    #   in the streaming buffer, which is not supported"
    # So we're using either DROP TABLE or copying
    if not audience_name:
      query = f'DROP TABLE `{table_fq_name}`'
      self.execute_query(query)
      self._ensure_table(table_fq_name, TableSchemas.audiences_log)
    else:
      # we're asked to rebuild logs only for one audience.
      # so basically we're recreating the table without rows for that audience
      query = f"""CREATE TABLE `{table_fq_name}_new` AS
SELECT * FROM {table_fq_name}
WHERE name != '{audience_name}';

DROP TABLE `{table_fq_name}`;

-- Rename the new table
ALTER TABLE `{table_fq_name}_new` RENAME TO audiences_log;
      """
      self.execute_query(query)

    result = {}
    # recreate log entries for each audience
    for audience in audiences:
      if audience_name and audience.name != audience_name:
        continue
      # we load existing log entries to restore relations with jobs
      audience_log_existing = audiences_log.get(audience.name, None)
      # NOTE: here we're not ignoring audiences with mode=off as usual
      audience_log = self.recalculate_audience_log(target, audience,
                                                   audience_log_existing)
      if audience_log:
        self.update_audiences_log(target, audience_log)
      result[audience_name] = audience_log
    return result

  def recalculate_audience_log(self,
                               target: ConfigTarget,
                               audience: Audience,
                               audience_log_existing: list[AudienceLog] = None):
    logger.info("Recalculating log for audience '%s'", audience.name)
    audience_log_existing = audience_log_existing or []
    table_users = self._get_user_segment_table_full_name(
        target, audience.table_name, 'test', '*')
    query = f'SELECT MIN(_TABLE_SUFFIX) AS start_day, MAX(_TABLE_SUFFIX) AS end_day FROM `{table_users}`'
    try:
      res = self.execute_query(query)
    except exceptions.NotFound:
      return
    if not res:
      return
    res = res[0]
    start_day = res['start_day']
    end_day = res['end_day']
    start_date = datetime.strptime(start_day, '%Y%m%d')
    end_date = datetime.strptime(end_day, '%Y%m%d')
    num_days = (end_date - start_date).days
    total_test_user_count = 0
    total_control_user_count = 0
    logs = []
    for day in range(num_days + 1):
      current_day = start_date + timedelta(days=day)
      test_user_count, control_user_count, new_test_user_count, new_control_user_count = \
        self.load_user_segment_stat(target, audience, current_day.strftime('%Y%m%d') )
      if not test_user_count:
        continue
      total_test_user_count += new_test_user_count
      total_control_user_count += new_control_user_count
      existing_same_day_entries = list([
          i for i in audience_log_existing
          if i.date.strftime('%Y%m%d') == current_day.strftime('%Y%m%d')
      ])
      entry = AudienceLog(
          name=audience.name,
          date=current_day,
          job_resource_name=existing_same_day_entries[0].job_resource_name
          if existing_same_day_entries and len(existing_same_day_entries) == 1
          else '',
          uploaded_user_count=test_user_count,
          new_test_user_count=new_test_user_count,
          new_control_user_count=new_control_user_count,
          test_user_count=test_user_count,
          control_user_count=control_user_count,
          total_test_user_count=total_test_user_count,
          total_control_user_count=total_control_user_count,
          failed_user_count=0,
      )
      logs.append(entry)
      logger.debug(entry)
    return logs
