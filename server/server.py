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
from google import oauth2
import json
import os
import math
import argparse
import yaml
import decimal
from datetime import datetime, date
from pprint import pprint
import traceback
from flask import Flask, request, jsonify, abort, send_from_directory, send_file, Response
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
from gaarf.api_clients import GoogleAdsApiClient
from gaarf.query_executor import AdsReportFetcher
import pandas as pd
import pandas_gbq
import numpy as np
import statsmodels.stats.proportion as proportion

from env import IS_GAE
from auth import get_credentials
from logger import logger
from context import Context
from config import Config, ConfigTarget, Audience, parse_arguments, get_config, save_config, AppNotInitializedError
from sampling import do_sampling
from ads_gateway import AdsGateway
from data_gateway import AudienceLog
from cloud_scheduler_gateway import Job

# NOTE: this module's code is executed each time when a new worker started, so keep it small
# To handle instance start up see `on_instance_start` method

class JsonEncoder(json.JSONEncoder):
  flask_default: Callable[[Any], Any]
  def default(self, obj):
    if isinstance(obj, Audience):
      return obj.to_dict()
    return JsonEncoder.flask_default(obj)

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
    kwargs.setdefault("cls", JsonEncoder)
    kwargs.setdefault("default", None)
    return DefaultJSONProvider.dumps(self, obj, **kwargs)


STATIC_DIR = os.getenv(
    'STATIC_DIR'
) or '../dist'  # folder for static content relative to the current module

Flask.json_provider_class = JSONProvider
app = Flask(__name__)
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


def _get_config(*, fail_ok = False) -> Config:
  # it can throw FileNotFoundError if config is missing
  config = get_config(args, fail_ok)
  return config


def create_context(target_name: str = None, *, fail_ok = False) -> Context:
  credentials = _get_credentials()
  config = _get_config(fail_ok=fail_ok)
  target_name = _get_req_arg_str('target') if target_name == None else target_name
  if not target_name:
    if not config.targets:
      target = None
    elif len(config.targets) == 1:
      # target is not provided, but there's onluy one
      target = config.targets[0]
    elif len(config.targets) > 1:
      # take a default one
      target = next(filter(lambda t: not t.name or t.name == 'default', config.targets), None)
      if not target:
        # otherwise just the first one
        target = config.targets[0]
  elif target_name:
    target = next(filter(lambda t: t.name == target_name, config.targets), None)
  else:
    target = None
  logger.debug(f"Created context for target: {target}")
  context = Context(config, target, credentials)
  return context


@app.route("/api/configuration", methods=["GET"])
def get_configuration():
  #context = create_context()
  config = _get_config()
  result = config.to_dict()

  # targets = [t.name for t in config.targets]
  # result = {
  #   "project_id": config.project_id,
  #   "name": target.name,
  #   "targets": targets,
  #   "ga4_project": context.target.ga4_project,
  #   "ga4_dataset": context.target.ga4_dataset,
  #   "ga4_table": context.target.ga4_table,
  #   "bq_dataset_id": context.target.bq_dataset_id,
  #   "bq_dataset_location": context.target.bq_dataset_location,
  #   "ads_customer_id": context.target.ads_customer_id,
  #   "ads_developer_token": context.target.ads_developer_token,
  #   "ads_client_id": context.target.ads_client_id,
  #   "ads_client_secret": context.target.ads_client_secret,
  #   "ads_refresh_token": context.target.ads_refresh_token,
  #   "ads_login_customer_id": context.target.ads_login_customer_id,
  #   #"scheduled": context.target.scheduled,
  #   #"schedule": context.target.schedule,
  #   #"schedule_timezone": context.target.schedule_timezone,
  # }
  logger.debug(f"returning configuration: {result}")
  return jsonify(result)


@app.route("/api/setup", methods=["POST"])
def setup():
  context = create_context(fail_ok=True)
  params = request.get_json(force=True)
  logger.info(f'Running setup with params:\n {params}')
  is_new = params.get('is_new', False)
  name = (params.get('name') or 'default').strip().lower()
  ga4_project = params.get('ga4_project', None)
  ga4_dataset = params.get('ga4_dataset', None)
  ga4_table = params.get('ga4_table', 'events')
  bq_dataset_id = params.get('bq_dataset_id', None)
  #bq_dataset_location = params.get('bq_dataset_location', None)
  context.config.targets = context.config.targets or []

  if not ga4_dataset:
    raise Exception(f'Please specify GA4 dataset')

  if is_new:
    # create new
    if next(filter(lambda t: t.name == name, context.config.targets), None):
      raise Exception(f'Configuration "{name}" already exists, please choose a different name')
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
      target = next(filter(lambda t: t.name == name_org, context.config.targets), None)
      if not target:
        if len(context.config.targets) == 1 and (not name_org or name_org == 'default'):
          target: ConfigTarget = context.config.targets[0]
        else:
          raise Exception(f'Configuration "{name_org}" does not exist')


  target.name = name
  target.ga4_project = ga4_project or context.config.project_id
  target.ga4_dataset = ga4_dataset
  target.ga4_table = ga4_table or 'events'
  #TODO: validate ga4_* parameters
  try:
    ds = context.data_gateway.bq_client.get_dataset(target.ga4_project + '.' + target.ga4_dataset)
    logger.debug(f"As GA4 dataset was specified ({ga4_dataset}) we'll use its location '{ds.location}' as the location for our dataset")
    bq_dataset_location = ds.location
  except BaseException as e:
    raise Exception(f'An error occurred while accessing GA4 dataset: {e}')

  target.bq_dataset_location = bq_dataset_location
  target.bq_dataset_id = bq_dataset_id or 'remarque'

  # targets should not be sharing same dataset
  for other in context.config.targets:
    if other != target:
      if other.bq_dataset_id == target.bq_dataset_id:
        raise Exception(f'Configuration "{name}" uses the same dataset "{target.bq_dataset_id}" as configuration "{other.name}", please choose a different one')

  context.data_gateway.initialize(target)

  target.ads_client_id = params.get('ads_client_id', None)
  target.ads_client_secret = params.get('ads_client_secret', None)
  target.ads_customer_id = params.get('ads_customer_id', None)
  target.ads_developer_token = params.get('ads_developer_token', None)
  target.ads_login_customer_id = params.get('ads_login_customer_id', None)
  target.ads_refresh_token = params.get('ads_refresh_token', None)

  if (target.ads_refresh_token):
    ads_cfg = _get_ads_config(target)
    _validate_googleads_config(ads_cfg, throw=True)

  # save config to the same location where it was read from
  logger.info(f'Saving new configuration:\n{context.config.to_dict()}')
  save_config(context.config, args)

  return jsonify(context.config.to_dict())


@app.route('/api/setup/delete', methods=['POST'])
def setup_delete_target():
  context = create_context()
  if not context.target:
    raise Exception(f'Configuration "{context.target}" was not found')

  credentials = _get_credentials()
  config = _get_config()
  target_name = _get_req_arg_str('target')
  logger.info(f'Deleting target {target_name}')
  target = next(filter(lambda t: t.name == target_name, config.targets), None)
  if target:
    if target.bq_dataset_id:
      context = Context(config, target, credentials)
      fully_qualified_dataset_id = f'{config.project_id}.{target.bq_dataset_id}'
      context.data_gateway.bq_client.delete_dataset(fully_qualified_dataset_id, True)
      logger.debug(f'Deleted target\'s dataset {fully_qualified_dataset_id}')
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
  logger.debug(f"Updating Ads API credentials from google-ads.yam file:\n {cfg}")
  cfg["use_proto_plus"] = True
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
  "client_id": context.target.ads_client_id,
  "client_secret": context.target.ads_client_secret,
  "customer_id": context.target.ads_customer_id,
  "developer_token": context.target.ads_developer_token,
  "login_customer_id": context.target.ads_login_customer_id,
  "refresh_token": context.target.ads_refresh_token
  }
  file_name ="/tmp/google_ads.yaml"
  with open(file_name, mode="w") as f:
    yaml.safe_dump(ads_config, f)
  return send_file(file_name, as_attachment=True, download_name="google-ads.yaml")


@app.route('/api/setup/validate_ads_cred', methods=['POST'])
def setup_validate_ads_cred():
  context = create_context()
  params = request.get_json(force=True)
  cfg = {
    "developer_token": params.get('ads_developer_token', None),
    "client_id": params.get('ads_client_id', None),
    "client_secret": params.get('ads_client_secret', None),
    "refresh_token": params.get('ads_refresh_token', None),
    "login_customer_id": str(params.get('ads_login_customer_id')),
    "customer_id": str(params.get('ads_customer_id')),
    "use_proto_plus": True
  }
  _validate_googleads_config(cfg, throw=True)
  return jsonify(cfg)


@app.route("/api/setup/connect_ga4", methods=["POST"])
def setup_connect_ga4():
  context = create_context()
  params = request.get_json(force=True)

  ga4_project = params.get('ga4_project') or context.config.project_id
  ga4_dataset = params.get('ga4_dataset')
  ga4_table = params.get('ga4_table') or 'events'
  ga_table = f'{ga4_project}.{ga4_dataset}.{ga4_table}_{datetime.today().strftime("%Y")}*'
  query = f"SELECT DISTINCT _TABLE_SUFFIX as table FROM `{ga_table}` ORDER BY 1 DESC LIMIT 10"
  try:
    response = context.data_gateway.execute_query(query)
    tables = [r["table"] for r in response]
    return jsonify({"results": tables})
  except BaseException as e:
    logger.error(e)
    sa = f"{context.config.project_id}@appspot.gserviceaccount.com"
    return jsonify({"error": {
      "message": f"Incorrect GA4 table name or the application's service account ({sa}) doesn't have access permission to the BigQuery dataset. Original error: {e}"
    }}), 400


@app.route("/api/stat", methods=["GET"])
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

  results = context.data_gateway.get_ga4_stats(context.target, days_ago_start, days_ago_end)
  return jsonify({"results": results})


@app.route("/api/audiences", methods=["GET"])
def get_audiences():
  context = create_context()
  if not context.target:
    raise AppNotInitializedError()
  audiences = context.data_gateway.get_audiences(context.target)
  return jsonify({"results": audiences})


@app.route("/api/audiences", methods=["POST"])
def update_audiences():
  context = create_context()
  pprint(request.args)
  params = request.get_json(force=True)
  audiences_raw = params["audiences"]
  logger.info("Updating audiences")
  logger.debug(audiences_raw)
  audiences = []
  for item in audiences_raw:
    audiences.append(Audience.from_dict(item))
  results = context.data_gateway.update_audiences(context.target, audiences)
  return jsonify({"results": results})


@app.route("/api/audiences/preview", methods=["POST"])
def calculate_users_for_audiences():
  context = create_context()
  pprint(request.args)
  params = request.get_json(force=True)
  audience_raw = params["audience"]
  logger.info("Previewing audience")
  logger.debug(audience_raw)
  audience = Audience.from_dict(audience_raw)
  df = context.data_gateway.fetch_audience_users(context.target, audience)
  # NOTE: we can return all users data if needed
  return jsonify({"users_count": len(df)})


@app.route("/api/audiences/get_query", methods=["POST"])
def get_query_for_audience() -> Response:
  context = create_context()
  params = request.get_json(force=True)
  audience_raw = params["audience"]
  logger.info("Previewing audience")
  logger.debug(audience_raw)
  audience = Audience.from_dict(audience_raw)
  query = context.data_gateway.get_audience_sampling_query(context.target, audience)
  return jsonify({"query": query})


@app.route("/api/audiences/base_conversion", methods=["POST", "GET"])
def get_base_conversion():
  context = create_context()
  params = request.get_json(force=True)
  audience_raw = params["audience"]
  audience = Audience.from_dict(audience_raw)
  date_start = params.get("date_start", None) or request.args.get('date_start')
  date_start = date.fromisoformat(date_start) if date_start else None
  date_end = params.get("date_end", None) or request.args.get('date_end')
  date_end = date.fromisoformat(date_end) if date_end else None
  conversion_window_days = params.get("conversion_window", None) or request.args.get('conversion_window')
  logger.info(f"Calculating baseline conversion for audience:\n {audience_raw}\nconversion_window={conversion_window_days}, date_start={date_start}, date_end={date_end}")

  result = context.data_gateway.get_base_conversion(context.target, audience, conversion_window_days, date_start, date_end)

  cr = float(result["cr"])
  logger.debug(f"Base conversion for audience is {cr}")
  return jsonify({"result": result})


@app.route("/api/audiences/power", methods=["POST", "GET"])
def get_power_analysis():
  from statsmodels.stats.power import TTestIndPower

  # parameters for power analysis
  cr = request.args.get('cr')
  if not cr:
    raise Exception("conversion rate (cr) was not specified")
  cr = float(cr)
  power = float(request.args.get('power') or 0.8)  # power
  alpha = float(request.args.get('alpha') or 0.05) # alpha
  ratio = float(request.args.get('ratio') or 1)    # ratio of test and control groups
  uplift = float(request.args.get('uplift') or 0.25)  # conversion uplift ratio
  p1 = cr
  p2 = cr * (1 + uplift)
  effect_size = (p1 - p2) / np.sqrt((p1*(1 - p1) + p2*(1 - p2)) / 2) #Cohen's h for proportions
  # Here p1 and p2 are the expected conversion rates in the control and treatment groups, respectively.

  analysis = TTestIndPower()
  sample_size = analysis.solve_power(effect_size=effect_size, power=power, alpha=alpha, ratio=ratio)
  sample_size = float(sample_size)
  logger.info(f"Power analysis calculation for parameters: cr={cr}, power={power}, alpha={alpha}, ratio={ratio}, uplift={uplift}, p1={p1}, p2={p2}, effect_size={effect_size}, the resulted sample_size={sample_size}")

  from statsmodels.stats.proportion import power_proportions_2indep
  # prop2 = cr  # base conversion rate
  # uplift = 0.25  # expect a 25% relative increase in conversion rate
  new_conversion_rate = cr * (1 + uplift)
  diff = new_conversion_rate - cr
  new_power = power_proportions_2indep(diff=diff, prop2=cr, nobs1=sample_size)
  logger.debug(f"new_conversion_rate={new_conversion_rate},diff={diff},new power={new_power}")

  return jsonify({
    "sample_size": int(sample_size),
    "new_power": new_power.power
  })


@app.route("/api/process", methods=["POST"])
def process():
  # it's a method for automated execution (via Cloud Scheduler)
  # TODO: currently it's not optimal as we load audiences twice and load test users though we have them at run_sampling stage
  run_sampling()
  update_customer_match_audiences()
  return jsonify({})


@app.route("/api/schedule", methods=["GET"])
def get_schedule():
  context = create_context()

  job = context.cloud_scheduler.get_job(context.target.name)
  logger.info(f'Loaded Schedule Job info: {job}')
  return jsonify({
    "scheduled": job.enabled,
    "schedule": job.schedule_time,
    "schedule_timezone": job.schedule_timezone
  })


@app.route("/api/schedule/edit", methods=["POST"])
def update_schedule():
  context = create_context()
  params = request.get_json(force=True)
  enabled = bool(params.get('scheduled', False))
  schedule = params.get('schedule')
  schedule_timezone = params.get('schedule_timezone')
  if enabled and not schedule:
    return jsonify({"error": {
      "message": "Schedule was not specified"
    }}), 400
  job = Job(enabled, schedule_timezone=schedule_timezone, schedule_time=schedule)
  logger.info(f'Updating Scheduler Job {job}')
  context.cloud_scheduler.update_job(context.target, job)
  return jsonify({})


@app.route("/api/sampling/run", methods=["GET", "POST"])
def run_sampling() -> Response:
  """
  Samples audiences. For each audience it does the following:
    - fetch users according to the audience definition
    - do sampling, i.e. split users onto test and control groups
  """
  context = create_context()
  context.data_gateway.ensure_user_normalized(context.target)
  audiences = context.data_gateway.get_audiences(context.target)
  result = {}
  logger.debug(f"Loaded {len(audiences)} audiences")
  for audience in audiences:
    # TODO: support 'prod' mode
    if audience.mode != 'test':
      continue
    logger.debug(f"Running sampling for '{audience.name}' audience")
    df = context.data_gateway.fetch_audience_users(context.target, audience)
    logger.info(f"Created a user segment of audience '{audience.name}' with {len(df)} users")
    if len(df) == 0:
      logger.warning("User segment of audience '{audience.name}' contains no users")

    # exclude old users from test and control groups from today's users
    old_test_users, old_ctrl_users = context.data_gateway.load_old_users(context.target, audience)
    mask = df['user'].isin(old_test_users['user']) | df['user'].isin(old_ctrl_users['user'])
    df_new = df[~mask]
    logger.debug(f"The segment has been adjusted to exclude old users and now contains {len(df_new)} users")
    # now df doesn't contain users from previous days

    # if the segment is empty there's no point in sampling
    # TODO: if audience.mode == 'prod':
    #
    if len(df_new) > 0:
      users_test, users_control = do_sampling(df_new)
    else:
      # create empty tables for test and control users so other queries wouldn't fail
      users_test = pd.DataFrame(columns=['user'])
      users_control = pd.DataFrame(columns=['user'])

    logger.debug(f"Using stratification the segment was split onto: test - {len(users_test)} users, control - {len(users_control)} users")
    # add old test/control users from df to the new test/control groups
    old_test_df = df[df['user'].isin(old_test_users['user'])]
    old_control_df = df[df['user'].isin(old_ctrl_users['user'])]

    # append old users to the new test/control groups
    users_test = pd.concat([users_test, old_test_df], ignore_index=True)
    users_control = pd.concat([users_control, old_control_df], ignore_index=True)
    #users_test = users_test.concat(old_test_df, ignore_index=True)
    #users_control = users_control.concat(old_control_df, ignore_index=True)
    logger.debug(f"The today's test/control groups were updated with previously exposed users: test - {len(users_test)} users, control - {len(users_control)} users")

    # TODO: add old tests users with TTL>0

    context.data_gateway.save_sampled_users(context.target, audience, users_test, users_control)

    result[audience.name] = {
      "test_count": len(users_test),
      "control_count": len(users_control),
    }
  return jsonify({"result": result})


def _validate_googleads_config(ads_config, *, throw=False):
  client = GoogleAdsApiClient(config_dict=ads_config)
  report_fetcher = AdsReportFetcher(client)
  try:
    report_fetcher.fetch("SELECT customer.id FROM customer", ads_config["customer_id"] or ads_config["login_customer_id"])
    return True
  except Exception as e:
    if throw:
      raise Exception(f"Validation of Ads credentials failed: {e}")
    return False


def _get_ads_config(target: ConfigTarget, assert_non_empty=False):
  ads_config = {
    "developer_token": target.ads_developer_token,
    "client_id": target.ads_client_id,
    "client_secret": target.ads_client_secret,
    "refresh_token": target.ads_refresh_token,
    "login_customer_id": str(target.ads_login_customer_id or target.ads_customer_id),
    "customer_id": str(target.ads_customer_id or target.ads_login_customer_id),
    "use_proto_plus": True
  }
  if assert_non_empty and (not ads_config["refresh_token"] or not ads_config["developer_token"]):
    raise AppNotInitializedError("Google Ads API credentials are not specified, please add them on Configuration page")

  return ads_config


@app.route("/api/ads/upload", methods=["POST"])
def update_customer_match_audiences():
  context = create_context()
  ads_config = _get_ads_config(context.target, True)
  ads_client = GoogleAdsApiClient(config_dict=ads_config)
  ads_gateway = AdsGateway(context.config, context.target, ads_client)
  logger.debug(f"Creating or loading existing user lists from Google Ads")
  audiences = context.data_gateway.get_audiences(context.target)
  user_lists = ads_gateway.create_customer_match_user_lists(audiences)
  logger.info(user_lists)
  # update user list resource names for audiences
  need_updating = False
  for list_name, res_name in user_lists.items():
    for audience in audiences:
      if audience.name == list_name:
        if audience.user_list != res_name:
          logger.error(f'An audience {list_name} already has user_list ({audience.user_list}) but not the expected one - {res_name}')
          need_updating = True
        if not audience.user_list:
          need_updating = True
        audience.user_list = res_name
  if need_updating:
    context.data_gateway.update_audiences(context.target, audiences)

  audiences_log = context.data_gateway.get_audiences_log(context.target)
  # upload audiences users to Google Ads as customer match user lists
  logger.debug(f"Uploading audiences users to Google Ads as customer match userlists")
  result = {}
  log = []
  for audience in audiences:
    # TODO: support 'prod' mode
    if audience.mode == 'off':
      continue

    audience_name = audience.name
    user_list_res_name = audience.user_list
    # load of users for 'today' table (audience_{listname}_test_yyyyMMdd)
    users = context.data_gateway.load_audience_segment(context.target, audience, 'test')

    # upload users to Google Ads
    if len(users) > 0:
      job_resource_name, failed_users, uploaded_users = \
          ads_gateway.upload_customer_match_audience(user_list_res_name, users, True)
      # update audience status in our tables, plus calculate some statistics
      test_user_count, control_user_count, new_test_user_count, new_control_user_count = \
          context.data_gateway.update_audience_segment_status(context.target, audience, None, failed_users)
    else:
      logger.warn(f"Audience '{audience.name}' segment has no users")
      job_resource_name = None
      failed_users = []
      uploaded_users = []
      test_user_count = 0
      control_user_count = 0
      new_test_user_count = 0
      new_control_user_count = 0

    # for debug reason save uplaoded users
    if len(uploaded_users):
      uploaded_users_mapped = [[id] for id in uploaded_users]
      df = pd.DataFrame(uploaded_users_mapped, columns=['user'])
      uploaded_table_name = context.data_gateway._get_user_segment_table_full_name(context.target, audience.table_name, 'uploaded')
      pandas_gbq.to_gbq(df[['user']], uploaded_table_name, context.config.project_id, if_exists='replace')

    total_test_user_count = 0
    total_control_user_count = 0
    # get total_user_count from the previous log entry if it exists and add new_user_count
    audience_log = audiences_log.get(audience_name, None)
    if not audience_log:
      total_test_user_count = new_test_user_count
      total_control_user_count = new_control_user_count
    else:
      total_test_user_count = audience_log[-1].total_user_count + new_test_user_count
      total_control_user_count = audience_log[-1].total_control_user_count + new_control_user_count


    result[audience_name] = {
      "job_resource_name": job_resource_name,
      "uploaded_user_count": len(uploaded_users),
      "new_test_user_count": new_test_user_count,
      "new_control_user_count": new_control_user_count,
      "failed_user_count": len(failed_users) if failed_users else 0,
      "test_user_count": test_user_count,
      "control_user_count": control_user_count,
      "total_test_user_count": total_test_user_count,
      "total_control_user_count": total_control_user_count
    }
    if new_test_user_count == 0:
      logger.warning(f'Audience segment for {audience_name} for {datetime.now().strftime("%Y-%m-%d")} contains no new users')
    log.append(AudienceLog(
      audience.name,
      datetime.now(),
      job_resource_name,
      len(uploaded_users),
      new_test_user_count, new_control_user_count,
      test_user_count, control_user_count,
      total_test_user_count, total_control_user_count
      )
    )

  context.data_gateway.update_audiences_log(context.target, log)
  return jsonify({"result": result})


@app.route("/api/audiences/status", methods=["GET"])
def get_audiences_status():
  context = create_context()
  ads_config = _get_ads_config(context.target, True)
  ads_client = GoogleAdsApiClient(config_dict=ads_config)
  ads_gateway = AdsGateway(context.config, context.target, ads_client)

  audiences = context.data_gateway.get_audiences(context.target)
  audiences_log = context.data_gateway.get_audiences_log(context.target)
  user_lists = [i.user_list for i in audiences if i.user_list]
  jobs_status = ads_gateway.get_userlist_jobs_status(user_lists)
  logger.debug(f"Loaded {len(jobs_status)} offline jobs, showing first 20:")
  if logger.isEnabledFor(logger.level):
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
    if audience.user_list:
      jobs_statuses = [(i['resource_name'], i['status'], i['failure_reason']) for i in jobs_status if i['user_list'] == audience.user_list]
    audience_dict['log'] = []
    if audience_log:
      audience_dict['log'] = []
      for log_item in audience_log:
        log_item_dict = {
          "date": log_item.date,
          "job": log_item.job,
          "user_count": log_item.user_count,
          "new_test_user_count": log_item.new_user_count,
          "new_control_user_count": log_item.new_control_user_count,
          "test_user_count": log_item.test_user_count,
          "control_user_count": log_item.control_user_count,
          "total_test_user_count": log_item.total_user_count,
          "total_control_user_count": log_item.total_control_user_count,
        }
        job_status = next(((j[1], j[2]) for j in jobs_statuses if j[0] == log_item.job), None)
        if not job_status is None:
          status, failure_reason = job_status
        else:
          status, failure_reason = None, None
        log_item_dict['job_status'] = status
        log_item_dict['job_failure'] = failure_reason if failure_reason != 'UNSPECIFIED' else ''
        audience_dict['log'].append(log_item_dict)

  return jsonify({"result": result})


@app.route("/api/audiences/conversions", methods=["GET"])
def get_user_conversions():
  context = create_context()

  date_start = request.args.get('date_start')
  date_start = date.fromisoformat(date_start) if date_start else None
  date_end = request.args.get('date_end')
  date_end = date.fromisoformat(date_end) if date_end else None
  country = request.args.get('country')
  if country:
    country = country.split(',')
  audiences = context.data_gateway.get_audiences(context.target)
  audience_name = request.args.get('audience')
  logger.info(f"Calculating conversions graph for '{audience_name}' audience and {date_start}-{date_end} timeframe")
  results = {}
  pval = None
  chi = None
  for audience in audiences:
    if audience_name and audience.name != audience_name:
      continue
    result, date_start, date_end = context.data_gateway.get_user_conversions(context.target, audience, date_start, date_end, country)
    # the result is a list of columns: date, cum_test_regs, cum_control_regs, total_user_count, total_control_user_count

    array_conversions = [ [i["cum_test_regs"], i["cum_control_regs"]] for i in result]
    array_users = [ [i["total_user_count"], i["total_control_user_count"]] for i in result]

    logger.debug(f"Calculating pval for\nconversions: {array_conversions}\nnumber of users: {array_users}")
    chi, pval, res = proportion.proportions_chisquare(array_conversions, array_users)
    logger.debug(f"Calculated pval: {pval}, chi: {chi}")

    results[audience.name] = {
      "conversions": result,
      "date_start": date_start.strftime("%Y-%m-%d"),
      "date_end": date_end.strftime("%Y-%m-%d"),
      "pval": pval if math.isfinite(pval) else None,
      "chi": chi if math.isfinite(chi) else None,
    }
    if audience_name and audience.name == audience_name:
      break

  return jsonify({
      "results": results
    })


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
  # NOTE: we don't use Flask standard support for static files
  # (static_folder option and send_static_file method)
  # because they can't distinguish requests for static files (js/css) and client routes (like /products)
  file_requested = os.path.join(app.root_path, STATIC_DIR, path)
  if not os.path.isfile(file_requested):
    path = "index.html"
  max_age = 0 if path == "index.html" else None
  response = send_from_directory(STATIC_DIR, path, max_age=max_age)
  # There is a "feature" in GAE - all files have zeroed timestamp ("Tue, 01 Jan 1980 00:00:01 GMT")
  if IS_GAE:
    response.headers.remove("Last-Modified")
  if path == "index.html":
    response.headers.remove("ETag")
  response.cache_control.no_cache = True
  response.cache_control.no_store = True
  logger.debug(f"Static file request {path} processed")
  return response


@app.errorhandler(Exception)
def handle_exception(e: Exception):
  logger.exception(e)
  if getattr(e, "errors", None):
    logger.error(e.errors)
  if request.accept_mimetypes.accept_json and request.path.startswith('/api/'):
    # NOTE: not all exceptions can be serialized
    error_type = type(e).__name__
    error_message = str(e)

    # format the error message with the traceback
    debug_info = ""
    if app.config["DEBUG"]:
      debug_info = "".join(traceback.format_tb(e.__traceback__))

    # create and return the JSON response
    response = jsonify({
        "error": {
          "type": error_type,
          "message": f"{error_type}: {error_message}",
          "debugInfo": debug_info
        }
    })
    response.status_code = 500
    return response
    # try:
    #   return jsonify({"error": e}), 500
    # except:
    #   return jsonify({"error": str(e)}), 500
  return e


# @app.route("/_ah/start")
# def on_instance_start():
#   """Instance start handler. It's called by GAE to start a new instance (not workers).

#   Be mindful about code here because errors won't be propagated to webapp users
#   (i.e. visible only in log)"""
#   pass


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
