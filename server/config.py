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

import argparse
import os
import json
from typing import List
import google.auth
from googleapiclient.discovery import build
from auth import get_credentials
import smart_open
from logger import logger
from env import GAE_LOCATION


class AppNotInitializedError(Exception):

  def __init__(self, msg=None) -> None:
    super().__init__(
        msg or
        'Application is not initialized, please go to Configuration page and run setup'
    )


class ConfigItemBase:

  def __init__(self):
    # copy class atrributes (with values) into the instance
    members = [
        attr for attr in dir(self)
        if not attr.startswith('__') and attr != 'update' and attr != 'validate'
    ]
    for attr in members:
      setattr(self, attr, getattr(self, attr))

  def update(self, kw):
    """Update current object with values from json/dict.
       Only know properties (i.e. those that exist in object's class as class attributes) are set
    """
    cls = type(self)
    for k in kw:
      if hasattr(cls, k):
        new_val = kw[k]
        def_val = getattr(cls, k)
        if (new_val == '' and def_val != ''):
          new_val = def_val
        setattr(self, k, new_val)


class ConfigTarget(ConfigItemBase):
  # target name (required)
  name: str = ''
  ga4_project = ''
  ga4_dataset = ''
  ga4_table = ''
  ga4_loopback_window: str = ''
  ga4_loopback_recreate: bool = False
  # dataset id for all tables
  bq_dataset_id: str = 'remarque'
  # location for dataset in BigQuery (readonly on the client)
  bq_dataset_location: str = ''
  # notification email
  notification_email = ''

  # Google Ads customer id
  ads_customer_id: str = ''
  ads_developer_token = ''
  ads_client_id = ''
  ads_client_secret = ''
  ads_refresh_token = ''
  ads_login_customer_id = ''


class Config(ConfigItemBase):
  # GCP project id
  project_id: str = ''
  scheduler_location_id = ''

  def __init__(self) -> None:
    self.targets: List[ConfigTarget] = []

  def to_dict(self) -> dict:
    values = {
        'project_id': self.project_id,
        'scheduler_location_id': self.scheduler_location_id,
        'targets': []
    }
    for t in self.targets:
      target_json = {
          'name': t.name,
          'ga4_project': t.ga4_project,
          'ga4_dataset': t.ga4_dataset,
          'ga4_table': t.ga4_table,
          'ga4_loopback_window': t.ga4_loopback_window,
          'ga4_loopback_recreate': t.ga4_loopback_recreate,
          'bq_dataset_id': t.bq_dataset_id,
          'bq_dataset_location': t.bq_dataset_location,
          'notification_email': t.notification_email,
          'ads_customer_id': t.ads_customer_id,
          'ads_developer_token': t.ads_developer_token,
          'ads_client_id': t.ads_client_id,
          'ads_client_secret': t.ads_client_secret,
          'ads_refresh_token': t.ads_refresh_token,
          'ads_login_customer_id': t.ads_login_customer_id
          #"period_start": t.period_start,
          #"period_end": t.period_end
      }
      values['targets'].append(target_json)
    return values

  def get_targets_names(self):
    return [t.name for t in self.targets]


def parse_arguments(only_known: bool = False) -> argparse.Namespace:
  """Initialize command line parser using argparse.

  Returns:
    An argparse.ArgumentParser.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument('--config', help='Config file path')
  parser.add_argument('--project_id', '--project-id', help='GCP project id.')

  #auth.add_auth_arguments(parser)
  if only_known:
    args = parser.parse_known_args()[0]
  else:
    args = parser.parse_args()
  args.config = args.config or os.environ.get('CONFIG') or 'config.json'
  return args


def find_project_id(args: argparse.Namespace):
  if getattr(args, 'project_id', ''):
    project_id = getattr(args, 'project_id')
  _, project_id = google.auth.default()
  return project_id


def get_config_url(args: argparse.Namespace):
  config_file_name = args.config or os.environ.get('CONFIG') or 'config.json'
  if config_file_name.find('$PROJECT_ID') > -1:
    project_id = find_project_id(args)
    if project_id is None:
      raise Exception(
          'Config file url contains macro $PROJECT_ID but project id isn\'t specified and can\'t be detected from environment'
      )
    config_file_name = config_file_name.replace('$PROJECT_ID', project_id)
  return config_file_name


def get_config(args: argparse.Namespace, fail_ok=False) -> Config:
  """
  Read config file and merge settings from it, command line args and env vars.

  Args:
    args: cli arguments
    fail_ok: pass true to raise an exception if config is invalid

  Returns: a config object
  """
  config_file_name = get_config_url(args)
  logger.info('Using config file %s', config_file_name)
  try:
    with smart_open.open(config_file_name, 'rb') as f:
      content = f.read()
  except (FileNotFoundError, google.cloud.exceptions.NotFound) as e:
    msg = f'Config file {config_file_name} was not found: {e}'
    logger.error(msg)
    if fail_ok:
      logger.warning(
          'Config file was not found but proceeding due to fail_ok=True flag')
      content = '{}'
    else:
      raise AppNotInitializedError(msg) from e

  cfg_dict: dict = json.loads(content)
  config = Config()
  config.update(cfg_dict)
  if cfg_dict.get('targets'):
    for target_dict in cfg_dict['targets']:
      target = ConfigTarget()
      target.update(target_dict)
      config.targets.append(target)

  if len(config.targets) == 1 and not config.targets[0].name:
    config.targets[0].name = 'default'

  # project id (CLI arg overwrites config)
  if getattr(args, 'project_id', ''):
    config.project_id = getattr(args, 'project_id')
  if not config.project_id:
    config.project_id = find_project_id(args)

  if not config.project_id:
    logger.error('We could not detect GCP project_id')
  else:
    logger.info('Project id: %s', config.project_id)

  if not config.scheduler_location_id:
    location_id = GAE_LOCATION
    if not location_id:
      credentials = get_credentials(args)
      service = build('appengine', 'v1', credentials=credentials)
      response = service.apps().get(appsId=config.project_id).execute()
      location_id = response.get('locationId')
    if location_id:
      # if Scheduler location isn't specified explicitly infer it from the GAE location
      # see https://cloud.google.com/appengine/docs/standard/locations
      # Two locations, which are called europe-west and us-central in App Engine commands and in the Google Cloud console, are called europe-west1 and us-central1, respectively, elsewhere in Google documentation.
      if location_id == 'europe-west' or location_id == 'us-central':
        config.scheduler_location_id = location_id + '1'
      else:
        config.scheduler_location_id = location_id
    else:
      logger.warning('We could not detect AppEngine location')
  logger.debug(config.to_dict())
  return config


def save_config(config: Config, args: argparse.Namespace):
  config_file_name = get_config_url(args)
  with smart_open.open(config_file_name, 'w') as f:
    config_dict = config.to_dict()
    # NOTE: we're not saving the following parameters as they can be detected in runtime
    #       and it's be more reliable than taking them from config
    del config_dict['project_id']
    del config_dict['scheduler_location_id']
    f.write(json.dumps(config_dict))
