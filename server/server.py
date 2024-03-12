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

from typing import Any, Callable
import json
import os
import math
import yaml
from datetime import datetime, date
from pprint import pprint
import traceback
from flask import Flask, request, jsonify, abort, send_from_directory, send_file, Response
from flask.json.provider import DefaultJSONProvider
from google.appengine.api import wrap_wsgi_app
from flask_cors import CORS

from gaarf.api_clients import GoogleAdsApiClient
from gaarf.query_executor import AdsReportFetcher
import numpy as np
import statsmodels.stats.proportion as proportion
from statsmodels.stats.power import TTestIndPower

from env import IS_GAE
from auth import get_credentials
from logger import logger
from context import Context, ContextOptions
from models import Audience
from config import Config, ConfigTarget, parse_arguments, get_config, save_config, AppNotInitializedError
from middleware import run_sampling_for_audience, upload_customer_match_audience, update_customer_match_mappings
from cloud_scheduler_gateway import Job
from mailer import send_email
from utils import format_duration

# NOTE: this module's code is executed each time when a new worker started, so keep it small


class JsonEncoder(json.JSONEncoder):
  flask_default: Callable[[Any], Any]

  def default(self, o):
    if isinstance(o, Audience):
      return o.to_dict()
    return JsonEncoder.flask_default(o)


class JSONProvider(DefaultJSONProvider):

  def dumps(self, obj: Any, **kwargs: Any) -> str:
    """Serialize data as JSON to a string.

    Keyword arguments are passed to :func:`json.dumps`. Sets some
    parameter defaults from the :attr:`default`,
    :attr:`ensure_ascii`, and :attr:`sort_keys` attributes.

    :param obj: The data to serialize.
    :param kwargs: Passed to :func:`json.dumps`.
    """
    JsonEncoder.flask_default = DefaultJSONProvider.default
    kwargs.setdefault('cls', JsonEncoder)
    kwargs.setdefault('default', None)
    return DefaultJSONProvider.dumps(self, obj, **kwargs)


STATIC_DIR = (os.getenv('STATIC_DIR') or '../dist'
             )  # folder for static content relative to the current module

Flask.json_provider_class = JSONProvider
app = Flask(__name__)
app.wsgi_app = wrap_wsgi_app(app.wsgi_app)

CORS(app)

args = parse_arguments(only_known=True)


def _get_req_arg_str(name: str):
  arg = request.args.get(name)
  if not arg or arg == 'null' or arg == 'undefined':
    return None
  return str(arg)


def _get_credentials():
  credentials = get_credentials(args)
  return credentials


def _get_config(*, fail_ok=False) -> Config:
  # it can throw FileNotFoundError if config is missing
  config = get_config(args, fail_ok)
  return config


def create_context(target_name: str = None,
                   *,
                   create_ads=False,
                   fail_ok=False) -> Context:
  credentials = _get_credentials()
  config = _get_config(fail_ok=fail_ok)
  target_name = _get_req_arg_str(
      'target') if target_name == None else target_name
  if not target_name:
    if not config.targets:
      target = None
    elif len(config.targets) == 1:
      # target is not provided, but there's only one
      target = config.targets[0]
    elif len(config.targets) > 1:
      # take a default one
      target = next(
          filter(lambda t: not t.name or t.name == 'default', config.targets),
          None)
      if not target:
        # otherwise just the first one
        target = config.targets[0]
  elif target_name:
    # a target is provided
    if not config.targets:
      target = None
    else:
      target = next(
          filter(lambda t: t.name == target_name, config.targets), None)
      if not target:
        # but wasn't found in the configuration
        if len(config.targets) == 1:
          target = config.targets[0]
        elif target_name == 'default':
          target = config.targets[0]
        else:
          raise ValueError(f'Unknown configuration name {target_name}')
  else:
    target = None

  context = Context(config, target, credentials,
                    ContextOptions(create_ads_gateway=create_ads))
  logger.debug('Created context for target: %s', target)
  return context


@app.route('/api/configuration', methods=['GET'])
def get_configuration():
  config = _get_config()
  result = config.to_dict()

  logger.debug('returning configuration: %s', result)
  return jsonify(result)


@app.route('/api/setup', methods=['POST'])
def setup():
  context = create_context(fail_ok=True)
  params = request.get_json(force=True)
  logger.info('Running setup with params:\n %s', params)
  is_new = params.get('is_new', False)
  name = (params.get('name') or 'default').strip().lower()
  ga4_project = params.get('ga4_project', None)
  ga4_dataset = params.get('ga4_dataset', None)
  ga4_table = params.get('ga4_table', 'events')
  bq_dataset_id = params.get('bq_dataset_id', None)
  context.config.targets = context.config.targets or []

  if not ga4_dataset:
    raise ValueError('Please specify GA4 dataset')

  if is_new:
    # create new
    if next(filter(lambda t: t.name == name, context.config.targets), None):
      raise ValueError(
          f'Configuration "{name}" already exists, please choose a different name'
      )
    target = ConfigTarget()
    context.config.targets.append(target)
  else:
    # edit
    if not context.config.targets:
      # edit the default target
      target = ConfigTarget()
      context.config.targets = [target]
    else:
      name_org = params.get('name_org')
      target = next(
          filter(lambda t: t.name == name_org, context.config.targets), None)
      if not target:
        if len(context.config.targets) == 1 and (not name_org or
                                                 name_org == 'default'):
          target: ConfigTarget = context.config.targets[0]
        else:
          raise Exception(f'Configuration "{name_org}" does not exist')

  target.name = name
  target.ga4_project = ga4_project or context.config.project_id
  target.ga4_dataset = ga4_dataset
  target.ga4_table = ga4_table or 'events'
  # TODO: validate ga4_* parameters
  try:
    ds = context.data_gateway.bq_client.get_dataset(target.ga4_project + '.' +
                                                    target.ga4_dataset)
    logger.debug(
        "As GA4 dataset was specified (%s) we'll use its location '%s' as the location for our dataset",
        ga4_dataset, ds.location)
    bq_dataset_location = ds.location
  except BaseException as e:
    raise Exception('"An error occurred while accessing GA4 dataset: {e}')

  target.bq_dataset_location = bq_dataset_location
  target.bq_dataset_id = bq_dataset_id or 'remarque'

  # targets should not be sharing same dataset
  for other in context.config.targets:
    if other != target:
      if other.bq_dataset_id == target.bq_dataset_id:
        raise Exception(
            f'Configuration "{name}" uses the same dataset "{target.bq_dataset_id}" as configuration "{other.name}", please choose a different one'
        )

  context.data_gateway.initialize(target)

  target.ads_client_id = params.get('ads_client_id', None)
  target.ads_client_secret = params.get('ads_client_secret', None)
  target.ads_customer_id = params.get('ads_customer_id', None)
  target.ads_developer_token = params.get('ads_developer_token', None)
  target.ads_login_customer_id = params.get('ads_login_customer_id', None)
  target.ads_refresh_token = params.get('ads_refresh_token', None)

  if target.ads_refresh_token:
    ads_cfg = _get_ads_config(target)
    _validate_googleads_config(ads_cfg, throw=True)

  # save config to the same location where it was read from
  logger.info('Saving new configuration:\n%s', context.config.to_dict())
  save_config(context.config, args)
  # TODO:
  # if name_org := params.get('name_org', None) and name_org != name:
  # remove a Schedule Job left
  #  context.cloud_scheduler.delete_job(context.config, name_org)

  return jsonify(context.config.to_dict())


@app.route('/api/setup/delete', methods=['POST'])
def setup_delete_target():
  context = create_context()
  if not context.target:
    raise Exception(f'Configuration "{context.target}" was not found')

  credentials = _get_credentials()
  config = _get_config()
  target_name = _get_req_arg_str('target')
  logger.info('Deleting target %s', target_name)
  target = next(filter(lambda t: t.name == target_name, config.targets), None)
  if target:
    if target.bq_dataset_id:
      context = Context(config, target, credentials)
      fully_qualified_dataset_id = f"{config.project_id}.{target.bq_dataset_id}"
      context.data_gateway.bq_client.delete_dataset(fully_qualified_dataset_id,
                                                    True)
      logger.debug("Deleted target's dataset %s", fully_qualified_dataset_id)
    config.targets.remove(target)

  save_config(config, args)
  return jsonify(config.to_dict())


@app.route('/api/setup/upload_ads_cred', methods=['POST'])
def setup_upload_ads_cred():
  context = create_context()
  names = request.files.keys()
  if not names:
    raise ValueError('Expected a file in google-ads.yaml format')
  name = next(iter(names))
  cfg = yaml.load(request.files[name].stream, Loader=yaml.SafeLoader)
  logger.debug('Updating Ads API credentials from google-ads.yam file:\n %s',
               cfg)
  cfg['use_proto_plus'] = True
  _validate_googleads_config(cfg, throw=True)

  context.target.ads_client_id = cfg['client_id']
  context.target.ads_client_secret = cfg['client_secret']
  context.target.ads_customer_id = cfg['customer_id']
  context.target.ads_developer_token = cfg['developer_token']
  context.target.ads_login_customer_id = cfg['login_customer_id']
  context.target.ads_refresh_token = cfg['refresh_token']

  save_config(context.config, args)
  return jsonify(cfg)


@app.route('/api/setup/download_ads_cred', methods=['GET'])
def setup_download_ads_cred():
  context = create_context()
  ads_config = {
      'client_id': context.target.ads_client_id,
      'client_secret': context.target.ads_client_secret,
      'customer_id': context.target.ads_customer_id,
      'developer_token': context.target.ads_developer_token,
      'login_customer_id': context.target.ads_login_customer_id,
      'refresh_token': context.target.ads_refresh_token,
  }
  file_name = '/tmp/google_ads.yaml'
  with open(file_name, mode='w') as f:
    yaml.safe_dump(ads_config, f)
  return send_file(
      file_name, as_attachment=True, download_name='google-ads.yaml')


@app.route('/api/setup/validate_ads_cred', methods=['POST'])
def setup_validate_ads_cred():
  context = create_context()
  params = request.get_json(force=True)
  cfg = {
      'developer_token': params.get('ads_developer_token', None),
      'client_id': params.get('ads_client_id', None),
      'client_secret': params.get('ads_client_secret', None),
      'refresh_token': params.get('ads_refresh_token', None),
      'login_customer_id': str(params.get('ads_login_customer_id')),
      'customer_id': str(params.get('ads_customer_id')),
      'use_proto_plus': True,
  }
  _validate_googleads_config(cfg, throw=True)
  return jsonify(cfg)


@app.route('/api/setup/connect_ga4', methods=['POST'])
def setup_connect_ga4():
  context = create_context()
  params = request.get_json(force=True)

  ga4_project = params.get('ga4_project') or context.config.project_id
  ga4_dataset = params.get('ga4_dataset')
  ga4_table = params.get('ga4_table') or 'events'
  try:
    tables = context.data_gateway.check_ga4(ga4_project, ga4_dataset, ga4_table)
    return jsonify({'results': tables})
  except BaseException as e:
    return jsonify({'error': str(e)}), 400


@app.route('/api/stat', methods=['GET'])
def get_stat() -> Response:
  context = create_context()

  days_ago_start = request.args.get('days_ago_start', type=int)
  days_ago_end = request.args.get('days_ago_end', type=int)
  if days_ago_start is None:
    error_message = {'error': 'days_ago_start is missing or not an integer'}
    return abort(400, error_message)
  if days_ago_end is None:
    error_message = {'error': 'days_ago_end is missing or not an integer'}
    return abort(400, error_message)

  results = context.data_gateway.get_ga4_stats(context.target, days_ago_start,
                                               days_ago_end)
  return jsonify({'results': results})


@app.route('/api/audiences', methods=['GET'])
def get_audiences():
  context = create_context()
  if not context.target:
    raise AppNotInitializedError()
  audiences = context.data_gateway.get_audiences(context.target)
  return jsonify({'results': audiences})


@app.route('/api/audiences', methods=['POST'])
def update_audiences():
  context = create_context()
  pprint(request.args)
  params = request.get_json(force=True)
  audiences_raw = params['audiences']
  logger.info('Updating audiences')
  logger.debug(audiences_raw)
  audiences = []
  for item in audiences_raw:
    audiences.append(Audience.from_dict(item))
  results = context.data_gateway.update_audiences(context.target, audiences)
  return jsonify({'results': results})


@app.route('/api/audience/preview', methods=['POST'])
def calculate_users_for_audience():
  context = create_context()
  pprint(request.args)
  params = request.get_json(force=True)
  audience_raw = params['audience']
  logger.info('Previewing audience:')
  logger.info(audience_raw)
  context.data_gateway.ensure_user_normalized(context.target)
  audience = Audience.from_dict(audience_raw)
  audience.ensure_table_name()
  query = context.data_gateway.get_audience_sampling_query(
      context.target, audience)
  rows = context.data_gateway.execute_query(query)

  return jsonify({'users_count': len(rows)})


@app.route('/api/audience/query', methods=['POST'])
def get_query_for_audience() -> Response:
  context = create_context()
  params = request.get_json(force=True)
  audience_raw = params['audience']
  logger.info('Previewing audience')
  logger.debug(audience_raw)
  audience = Audience.from_dict(audience_raw)
  query = context.data_gateway.get_audience_sampling_query(
      context.target, audience)
  return jsonify({'query': query})


@app.route('/api/audience/base_conversion', methods=['POST', 'GET'])
def get_base_conversion():
  context = create_context()
  params = request.get_json(force=True)
  audience_raw = params['audience']
  audience = Audience.from_dict(audience_raw)
  date_start = params.get('date_start', None) or request.args.get('date_start')
  date_start = date.fromisoformat(date_start) if date_start else None
  date_end = params.get('date_end', None) or request.args.get('date_end')
  date_end = date.fromisoformat(date_end) if date_end else None
  conversion_window_days = params.get(
      'conversion_window', None) or request.args.get('conversion_window')
  if conversion_window_days:
    conversion_window_days = int(conversion_window_days)
  logger.info(
      'Calculating baseline conversion for audience:\n %s\nconversion_window=%s, date_start=%s, date_end=%s',
      audience_raw, conversion_window_days, date_start, date_end)

  result = context.data_gateway.get_base_conversion(context.target, audience,
                                                    conversion_window_days,
                                                    date_start, date_end)

  logger.debug('Base conversion for audience is %s', result['cr'])
  return jsonify({'result': result})


@app.route('/api/audience/power', methods=['POST', 'GET'])
def get_power_analysis():
  # parameters for power analysis
  cr = request.args.get('cr')
  if not cr:
    raise ValueError('conversion rate (cr) was not specified')
  cr = float(cr)
  power = float(request.args.get('power') or 0.8)  # power
  alpha = float(request.args.get('alpha') or 0.05)  # alpha
  ratio = float(request.args.get('ratio') or
                1)  # ratio of test and control groups
  uplift = float(request.args.get('uplift') or 0.25)  # conversion uplift ratio
  p1 = cr
  p2 = cr * (1 + uplift)
  effect_size = (p1 - p2) / np.sqrt(
      (p1 * (1 - p1) + p2 * (1 - p2)) / 2)  # Cohen's h for proportions
  # Here p1 and p2 are the expected conversion rates in the control and treatment groups, respectively.

  analysis = TTestIndPower()
  sample_size = analysis.solve_power(
      effect_size=effect_size, power=power, alpha=alpha, ratio=ratio)
  sample_size = float(sample_size)
  logger.info(
      'Power analysis calculation for parameters: cr=%s, power=%s, alpha=%s, ratio=%s, uplift=%s, p1=%s, p2=%s, effect_size=%s, the resulted sample_size=%s',
      cr, power, alpha, ratio, uplift, p1, p2, effect_size, sample_size)

  # prop2 = cr  # base conversion rate
  # uplift = 0.25  # expect a 25% relative increase in conversion rate
  new_conversion_rate = cr * (1 + uplift)
  diff = new_conversion_rate - cr
  new_power = proportion.power_proportions_2indep(
      diff=diff, prop2=cr, nobs1=sample_size)
  logger.debug('new_conversion_rate=%s, diff=%s, new power=%s',
               new_conversion_rate, diff, new_power)

  return jsonify({
      'sample_size': int(sample_size),
      'new_power': new_power.power
  })


@app.route('/api/process', methods=['POST'])
def process():
  # it's a method for automated execution (via Cloud Scheduler)

  context = create_context(create_ads=True)
  if not context.target:
    raise Exception(
        f"Target was not set, target uri argument: {_get_req_arg_str('target')},"
        f"available targets in the configuration: {context.config.get_targets_names()}"
    )

  ts_start = datetime.now()
  logger.info("Starting automated processing for target '%s'",
              context.target.name)

  # TODO: wrap in try--catch to send any error to email
  context.data_gateway.ensure_user_normalized(context.target)
  audiences = context.data_gateway.get_audiences(context.target)
  audiences_log = context.data_gateway.get_audiences_log(context.target)
  update_customer_match_mappings(context, audiences)
  result = {}
  log = []
  logger.debug('Loaded %s audiences with logs', len(audiences))
  for audience in audiences:
    if audience.mode == 'off':
      continue
    users_test, users_control = run_sampling_for_audience(context, audience)
    audience_log = audiences_log.get(audience.name, None)
    log_item = upload_customer_match_audience(context, audience, audience_log,
                                              users_test)
    log.append(log_item)
    result[audience.name] = log_item.to_dict()

  if log:
    context.data_gateway.update_audiences_log(context.target, log)

  elapsed = datetime.now() - ts_start
  if IS_GAE and context.target.notification_email:
    subject = f"Remarque {'[' + context.target.name + ']' if context.target.name != 'default' else ''} - sampling completed"
    body = f"Processing of {len(result)} audiences has been completed.\n"
    for val in log:
      body += f"""\nAudience '{val.name}':
      Users sampled: {val.test_user_count}, {val.uploaded_user_count} of which uploaded to Ads ({val.failed_user_count} failed)
      Control users: {val.control_user_count}
      New test users: {val.new_test_user_count}
      New control users: {val.new_control_user_count}
      In total:
      test users for all days: {val.total_test_user_count}
      control users for all days: {val.total_control_user_count}

      Ads upload job resource name: {val.job_resource_name}
      """
    body += f"""\n Processing took {format_duration(elapsed)}"""
    send_email(context.config, context.target.notification_email, subject, body)
  return jsonify({'result': result})


@app.route('/api/schedule', methods=['GET'])
def get_schedule():
  context = create_context()

  job = context.cloud_scheduler.get_job(context.target.name)
  logger.info('Loaded Schedule Job info: %s', job)
  return jsonify({
      'scheduled': job.enabled,
      'schedule': job.schedule_time,
      'schedule_timezone': job.schedule_timezone,
      'schedule_email': context.target.notification_email,
  })


@app.route('/api/schedule/edit', methods=['POST'])
def update_schedule():
  context = create_context()
  params = request.get_json(force=True)
  enabled = bool(params.get('scheduled', False))
  schedule = params.get('schedule')
  schedule_timezone = params.get('schedule_timezone')
  schedule_email = params.get('schedule_email')
  if enabled and not schedule:
    return jsonify({'error': {'message': 'Schedule was not specified'}}), 400
  job = Job(
      enabled, schedule_timezone=schedule_timezone, schedule_time=schedule)
  logger.info('Updating Scheduler Job: %s, location_id: %s', job,
              context.config.scheduler_location_id)
  context.cloud_scheduler.update_job(context.target, job)
  if context.target.notification_email != schedule_email:
    context.target.notification_email = schedule_email
    save_config(context.config, args)

  return jsonify({})


@app.route('/api/sampling/run', methods=['GET', 'POST'])
def run_sampling() -> Response:
  """
  Samples audiences. For each audience it does the following:
    - fetch users according to the audience definition
    - do sampling, i.e. split users onto test and control groups
  """
  logger.info('Run sampling for all audiences')
  context = create_context()
  params = request.get_json(force=True)
  audience_name = request.args.get('audience', None) or params.get(
      'audience', None)
  context.data_gateway.ensure_user_normalized(context.target)
  audiences = context.data_gateway.get_audiences(context.target)
  result = {}
  logger.debug('Loaded %s audiences', len(audiences))
  for audience in audiences:
    if audience_name and audience.name != audience_name:
      continue
    if audience.mode == 'off':
      continue
    users_test, users_control = run_sampling_for_audience(context, audience)
    result[audience.name] = {
        'test_count': len(users_test),
        'control_count': len(users_control),
    }
  logger.info('Sampling completed with result: %s', result)

  return jsonify({'result': result})


def _validate_googleads_config(ads_config, *, throw=False):
  client = GoogleAdsApiClient(config_dict=ads_config)
  report_fetcher = AdsReportFetcher(client)
  try:
    report_fetcher.fetch(
        'SELECT customer.id FROM customer',
        ads_config['customer_id'] or ads_config['login_customer_id'],
    )
    return True
  except Exception as e:
    if throw:
      raise Exception(f'Validation of Ads credentials failed: {e}') from None
    return False


def _get_ads_config(target: ConfigTarget, assert_non_empty=False):
  ads_config = {
      'developer_token':
          target.ads_developer_token,
      'client_id':
          target.ads_client_id,
      'client_secret':
          target.ads_client_secret,
      'refresh_token':
          target.ads_refresh_token,
      'login_customer_id':
          str(target.ads_login_customer_id or target.ads_customer_id),
      'customer_id':
          str(target.ads_customer_id or target.ads_login_customer_id),
      'use_proto_plus':
          True,
  }
  if assert_non_empty and (not ads_config['refresh_token'] or
                           not ads_config['developer_token']):
    raise AppNotInitializedError(
        'Google Ads API credentials are not specified, please add them on Configuration page'
    )

  return ads_config


@app.route('/api/ads/upload', methods=['POST'])
def update_customer_match_audiences():
  logger.info('Uploading audiences to Google Ads')
  context = create_context(create_ads=True)
  params = request.get_json(force=True)
  audience_name = request.args.get('audience', None) or params.get(
      'audience', None)
  audiences = context.data_gateway.get_audiences(context.target)
  audiences_log = context.data_gateway.get_audiences_log(context.target)
  update_customer_match_mappings(context, audiences)
  # upload audiences users to Google Ads as customer match user lists
  logger.debug(
      'Uploading audiences users to Google Ads as customer match userlists')
  result = {}
  log = []
  for audience in audiences:
    if audience_name and audience.name != audience_name:
      continue
    if audience.mode == 'off':
      continue

    audience_log = audiences_log.get(audience.name, None)
    # load of users for 'today' table (audience_{listname}_test_yyyyMMdd)
    users = context.data_gateway.load_audience_segment(context.target, audience,
                                                       'test')
    log_item = upload_customer_match_audience(context, audience, audience_log,
                                              users)
    log.append(log_item)
    result[audience.name] = log_item.to_dict()

  context.data_gateway.update_audiences_log(context.target, log)
  return jsonify({'result': result})


@app.route('/api/audiences/status', methods=['GET'])
def get_audiences_status():
  context = create_context(create_ads=True)
  include_log_duplicates = (
      request.args.get('include_log_duplicates', type=str) == 'true')
  audiences = context.data_gateway.get_audiences(context.target)
  audiences_log = context.data_gateway.get_audiences_log(
      context.target, include_duplicates=include_log_duplicates)
  user_lists = [
      i.user_list for i in audiences if i.user_list and i.mode != 'off'
  ]
  skip_ads_loading = request.args.get('skip_ads', type=str) == 'true'
  if skip_ads_loading:
    jobs_status = []
    campaigns = []
  else:
    jobs_status = context.ads_gateway.get_userlist_jobs_status(user_lists)
    user_lists_names = [i.name for i in audiences if i.user_list]
    campaigns = context.ads_gateway.get_userlist_campaigns(user_lists_names)
  if logger.isEnabledFor(logger.level):
    logger.debug('Loaded %s offline jobs, showing first 20:', len(jobs_status))
    logger.debug(jobs_status[:20])
  # resource_name, status, failure_reason, user_list

  # now we'll join all three info sources (audiences, audiences_logs, jobs_status)
  result = {}
  for audience in audiences:
    name = audience.name
    # all jobs for the currect audience
    audience_log = audiences_log.get(name, None)
    audience_dict = audience.to_dict()
    result[name] = audience_dict
    jobs_statuses = []
    audience_campaigns = []
    if audience.user_list:
      jobs_statuses = [(i['resource_name'], i['status'], i['failure_reason'])
                       for i in jobs_status
                       if i['user_list'] == audience.user_list]
      audience_campaigns = [
          i for i in campaigns if i.user_list_name == audience.name
      ]
    audience_dict['log'] = []
    audience_dict['campaigns'] = [{
        'customer_id':
            i.customer_id,
        'customer_name':
            i.customer_name,
        'campaign_id':
            i.campaign_id,
        'campaign_name':
            i.campaign_name,
        'campaign_status':
            i.campaign_status,
        'campaign_start_date':
            i.campaign_start_date,
        'campaign_end_date':
            i.campaign_end_date,
        'ad_group_id':
            i.ad_group_id,
        'ad_group_name':
            i.ad_group_name,
        'ad_group_status':
            i.ad_group_status,
        'user_list_id':
            i.user_list_id,
        'user_list_name':
            i.user_list_name,
        'user_list_description':
            i.user_list_description,
        'user_list_size_for_search':
            i.user_list_size_for_search,
        'user_list_size_for_display':
            i.user_list_size_for_display,
        'user_list_eligible_for_search':
            i.user_list_eligible_for_search,
        'user_list_eligible_for_display':
            i.user_list_eligible_for_display,
        'customer_link':
            f'https://ads.google.com/aw/overview?ocid={i.ocid}',
        'campaign_link':
            f'https://ads.google.com/aw/adgroups?campaignId={i.campaign_id}&ocid={i.ocid}',
        'ad_group_link':
            f'https://ads.google.com/aw/audiences/summary?campaignId={i.campaign_id}&adGroupId={i.ad_group_id}&ocid={i.ocid}',
        'user_list_link':
            f'https://ads.google.com/aw/audiences/management/details?ocid={i.ocid}&userListId={i.user_list_id}',
    } for i in audience_campaigns]
    if audience_log:
      audience_dict['log'] = []
      for log_item in audience_log:
        log_item_dict = {
            'date': log_item.date,
            'job': log_item.job_resource_name,
            'user_count': log_item.uploaded_user_count,
            'new_test_user_count': log_item.new_test_user_count,
            'new_control_user_count': log_item.new_control_user_count,
            'test_user_count': log_item.test_user_count,
            'control_user_count': log_item.control_user_count,
            'total_test_user_count': log_item.total_test_user_count,
            'total_control_user_count': log_item.total_control_user_count,
        }
        job_status = next(
            ((j[1], j[2])
             for j in jobs_statuses
             if j[0] == log_item.job_resource_name),
            None,
        )
        if not job_status is None:
          status, failure_reason = job_status
        else:
          status, failure_reason = None, None
        log_item_dict['job_status'] = status
        log_item_dict['job_failure'] = (
            failure_reason if failure_reason != 'UNSPECIFIED' else '')
        audience_dict['log'].append(log_item_dict)

  return jsonify({'result': result})


@app.route('/api/audiences/recalculate_log', methods=['POST'])
def recalculate_audiences_status():
  context = create_context(create_ads=True)
  context.data_gateway.rebuilt_audiences_log(context.target)
  return jsonify({})


@app.route('/api/conversions/query', methods=['GET'])
def get_conversions_query():
  context = create_context()
  date_start = request.args.get('date_start')
  date_start = date.fromisoformat(date_start) if date_start else None
  date_end = request.args.get('date_end')
  date_end = date.fromisoformat(date_end) if date_end else None
  country = request.args.get('country')
  if country:
    country = country.split(',')
  events = request.args.get('events')
  if events:
    events = events.split(',')
  audience_name = request.args.get('audience')
  if not audience_name:
    raise ValueError('No audience name was specified')
  logger.info(
      "Generating query for conversions for '%s' audience and %s-%s timeframe",
      audience_name, date_start, date_end)
  audiences = context.data_gateway.get_audiences(context.target, audience_name)
  if not audiences:
    raise ValueError(f"No audience with name '{audience_name}' found")
  audience = audiences[0]
  query, date_start, date_end = context.data_gateway.get_user_conversions_query(
      context.target, audience, date_start, date_end, country, events)
  return jsonify({
      'query': query,
      'date_start': date_start.strftime('%Y-%m-%d'),
      'date_end': date_end.strftime('%Y-%m-%d'),
  })


@app.route('/api/conversions', methods=['POST'])
def get_user_conversions():
  context = create_context(create_ads=True)
  params = request.get_json(force=True)

  date_start = params.get('date_start')
  date_start = date.fromisoformat(date_start) if date_start else None
  date_end = params.get('date_end')
  date_end = date.fromisoformat(date_end) if date_end else None
  country = params.get('country')
  if country:
    country = country.split(',')
  events = params.get('events')
  if events:
    events = [e.strip() for e in events.split(',') if e.strip()]
  audiences = context.data_gateway.get_audiences(context.target)
  audience_name = params.get('audience')
  logger.info(
      "Calculating conversions graph for '%s' audience and %s-%s timeframe",
      audience_name,
      date_start,
      date_end,
  )
  campaigns = params.get('campaigns', None)

  results = {}
  pval = None
  chi = None
  for audience in audiences:
    if audience_name and audience.name != audience_name:
      continue
    result, date_start, date_end = context.data_gateway.get_user_conversions(
        context.target, audience, date_start, date_end, country, events)
    # the result is a list of columns: date, cum_test_regs, cum_control_regs,
    # total_user_count, total_control_user_count

    if result:
      last_day_result = result[-1]
      logger.debug('Calculating pval for %s', last_day_result)
      chi, pval, res = proportion.proportions_chisquare(
          [
              int(last_day_result['cum_test_regs']),
              int(last_day_result['cum_control_regs']),
          ],
          [
              int(last_day_result['total_user_count']),
              int(last_day_result['total_control_user_count']),
          ],
      )
      logger.debug('Calculated pval: %s, chi: %s', pval, chi)
    else:
      logger.warning('get_user_conversions returned an empty result')
      pval = None
      chi = None

    # fetch campaign(s) metrics
    ads_metrics = None
    if campaigns:
      logger.debug('Loading Ads campaigns metrics')
      logger.debug(campaigns)
      cids = set([c['customer_id'] for c in campaigns])
      if cids and len(cids) > 1:
        logger.warning('More than one customer id provided: %s', cids)
      for cid in cids:
        campaign_ids = set([
            str(c['campaign_id']) for c in campaigns if c['customer_id'] == cid
        ])
        ads_metrics = context.ads_gateway.get_userlist_campaigns_metrics(
            cid,
            campaign_ids,
            date_start.strftime('%Y-%m-%d'),
            date_end.strftime('%Y-%m-%d'),
        )
        ads_metrics = [{
            'campaign':
                i['campaign_id'],
            'date':
                date.fromisoformat(i['date']),
            'unique_users':
                i['unique_users'],
            'clicks':
                i['clicks'],
            'average_impression_frequency_per_user':
                i['average_impression_frequency_per_user'],
        } for i in ads_metrics]
    logger.debug(ads_metrics)

    results[audience.name] = {
        'conversions': result,
        'ads_metrics': ads_metrics,
        'date_start': date_start.strftime('%Y-%m-%d'),
        'date_end': date_end.strftime('%Y-%m-%d'),
        'pval': pval if pval is not None and math.isfinite(pval) else None,
        'chi': chi if chi is not None and math.isfinite(chi) else None,
    }
    if audience_name and audience.name == audience_name:
      break

  return jsonify({'results': results})


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
  # NOTE: we don't use Flask standard support for static files
  # (static_folder option and send_static_file method)
  # because they can't distinguish requests for static files (js/css) and client routes (like /products)
  file_requested = os.path.join(app.root_path, STATIC_DIR, path)
  if not os.path.isfile(file_requested):
    path = 'index.html'
  max_age = 0 if path == 'index.html' else None
  response = send_from_directory(STATIC_DIR, path, max_age=max_age)
  # There is a "feature" in GAE - all files have zeroed timestamp ("Tue, 01 Jan 1980 00:00:01 GMT")
  if IS_GAE:
    response.headers.remove('Last-Modified')
  if path == 'index.html':
    response.headers.remove('ETag')
  response.cache_control.no_cache = True
  response.cache_control.no_store = True
  logger.debug('Static file request %s processed', path)
  return response


@app.errorhandler(Exception)
def handle_exception(e: Exception):
  logger.exception(e)
  if getattr(e, 'errors', None):
    logger.error(e.errors)
  if request.accept_mimetypes.accept_json and request.path.startswith('/api/'):
    # NOTE: not all exceptions can be serialized
    error_type = type(e).__name__
    error_message = str(e)

    # format the error message with the traceback
    debug_info = ''
    if app.config['DEBUG']:
      debug_info = ''.join(traceback.format_tb(e.__traceback__))

    # create and return the JSON response
    response = jsonify({
        'error': {
            'type': error_type,
            'message': f'{error_type}: {error_message}',
            'debugInfo': debug_info,
        }
    })
    response.status_code = 500
    return response
    # try:
    #   return jsonify({"error": e}), 500
    # except:
    #   return jsonify({"error": str(e)}), 500
  return e


if __name__ == '__main__':
  # This is used when running locally only. When deploying to Google App
  # Engine, a webserver process such as Gunicorn will serve the app. This
  # can be configured by adding an `entrypoint` to app.yaml.
  app.run(host='127.0.0.1', port=8080, debug=True)
