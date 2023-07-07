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
import argparse
import decimal
from datetime import datetime, date
from pprint import pprint
import traceback
from flask import Flask, request, jsonify, abort, send_from_directory, Response
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
from gaarf.api_clients import GoogleAdsApiClient

from env import IS_GAE
from auth import get_credentials
from logger import logger
from context import Context
from config import Config, ConfigTarget, Audience, parse_arguments, get_config, save_config
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


def _get_config() -> Config:
  # it can throw FileNotFoundError if config is missing
  config = get_config(args)
  return config


def create_context(target_name: str = None) -> Context:
  credentials = _get_credentials()
  config = _get_config()
  target_name = _get_req_arg_str('target') if target_name == None else target_name
  if not target_name and len(config.targets) == 1:
    target = config.targets[0]
  elif target_name:
    target = next(filter(lambda t: t.name == target_name, config.targets), None)
  else:
    target = None
  context = Context(config, target, credentials)
  return context


@app.route("/api/configuration", methods=["GET"])
def get_configuration():
  context = create_context()
  result = {
    "project_id": context.config.project_id,
    "bq_dataset_location": context.config.bq_dataset_location,
    "name": context.target.name,
    "ga4_project": context.target.ga4_project,
    "ga4_dataset": context.target.ga4_dataset,
    "ga4_table": context.target.ga4_table,
    "bq_dataset_id": context.target.bq_dataset_id,
    "ads_customer_id": context.target.ads_customer_id,
    "ads_developer_token": context.target.ads_developer_token,
    "ads_client_id": context.target.ads_client_id,
    "ads_client_secret": context.target.ads_client_secret,
    "ads_refresh_token": context.target.ads_refresh_token,
    "ads_login_customer_id": context.target.ads_login_customer_id,
    #"scheduled": context.target.scheduled,
    #"schedule": context.target.schedule,
    #"schedule_timezone": context.target.schedule_timezone,
  }
  return jsonify(result)


@app.route("/api/setup", methods=["POST"])
def setup():
  context = create_context()
  params = request.get_json(force=True)
  name = params.get('name')
  context.config.bq_dataset_location = params.get('bq_dataset_location', None)
  ga4_project = params.get('ga4_project', None)
  ga4_dataset = params.get('ga4_dataset', None)
  ga4_table = params.get('ga4_table', None)
  bq_dataset_id = params.get('bq_dataset_id', None)
  if not context.config.targets:
    target = ConfigTarget()
    context.config.targets = [target]
  else:
    target = next(filter(lambda t: t.name == name, context.config.targets), None)
    if not target:
      target: ConfigTarget = context.config.targets[0]

  target.name = name
  target.ga4_project = ga4_project
  target.ga4_dataset = ga4_dataset
  target.ga4_table = ga4_table
  target.bq_dataset_id = bq_dataset_id or 'remarque'
  #bq_dataset_location = context.config.bq_dataset_location or 'europe'

  context.data_gateway.initialize(target)
  # save config to the same location where it was read from
  save_config(context.config, args)
  return jsonify({})


@app.route("/api/setup/connect_ga4", methods=["POST"])
def setup_connect_ga4():
  context = create_context()
  params = request.get_json(force=True)
  if not context.config.targets:
    target = ConfigTarget()
    context.config.targets = [target]
  else:
    target: ConfigTarget = context.config.targets[0]

  ga4_project = target.ga4_project
  ga4_dataset = target.ga4_dataset
  ga4_table = target.ga4_table
  target.ga4_project = params.get('ga4_project')
  target.ga4_dataset = params.get('ga4_dataset')
  target.ga4_table = params.get('ga4_table')
  ga_table = context.data_gateway.get_ga4_table_name(context.target, True)
  query = f"SELECT DISTINCT _TABLE_SUFFIX as table FROM `{ga_table}` ORDER BY 1 DESC LIMIT 10"
  try:
    response = context.data_gateway.execute_query(query)
    tables = [r["table"] for r in response]
    # save config to the same location where it was read from
    save_config(context.config, args)
    return jsonify({"results": tables})
  except BaseException as e:
    target.ga4_project = ga4_project
    target.ga4_dataset = ga4_dataset
    target.ga4_table = ga4_table
    return jsonify({"error": {
      "message": "Incorrect GA4 table name"
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
  context = create_context()
  audiences = context.data_gateway.get_audiences(context.target)
  result = {}
  logger.debug(f"Loaded {len(audiences)} audiences")
  for audience in audiences:
    if not audience.active:
      logger.debug(f"Skipping non-active audience {audience.name}")
      continue
    logger.debug(f"Running sampling for '{audience.name}' audience")
    df = context.data_gateway.sample_audience_users(context.target, audience)
    logger.info(f"Created a user segment of audience '{audience.name}' with {len(df)} users")
    # if the segment is empty there's no point in sampling
    if len(df) > 0:
      users_test, users_control = do_sampling(df)
      context.data_gateway.save_sampled_users(context.target, audience, users_test, users_control)
    result[audience.name] = {
      "test_count": len(users_test) if len(df) > 0 else 0,
      "control_count": len(users_control) if len(df) > 0 else 0,
    }
  return jsonify({"result": result})


def _validate_googleads_config(ads_config):
  # TODO:
  return True

def _get_ads_config(target: ConfigTarget, validate_config = True):
  ads_config = {
    "developer_token": target.ads_developer_token,
    "client_id": target.ads_client_id,
    "client_secret": target.ads_client_secret,
    "refresh_token": target.ads_refresh_token,
    "login_customer_id": target.ads_login_customer_id or target.ads_customer_id,
    "customer_id": target.ads_customer_id or target.ads_login_customer_id,
    "use_proto_plus": True
  }
  if validate_config and not _validate_googleads_config(ads_config):
    raise Exception("Incomplete Google Ads configuration")
    #return jsonify({"error": "Incomplete Google Ads configuration"})
  return ads_config


@app.route("/api/ads/upload", methods=["POST"])
def update_customer_match_audiences():
  context = create_context()
  ads_config = _get_ads_config(context.target)
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

  # upload audiences users to Google Ads as customer match user lists
  logger.debug(f"Uploading audiences users to Google Ads as customer match userlists")
  result = {}
  log = []
  for audience in audiences:
    if not audience.active:
      logger.debug(f"Skipping non-active audience {audience.name}")
      continue

    audience_name = audience.name
    user_list_res_name = audience.user_list
    # load of users for 'today' table (audience_{listname}_test_yyyyMMdd)
    users = context.data_gateway.load_audience_segment(context.target, audience)
    job_resource_name, failed_users, uploaded_users = \
        ads_gateway.upload_customer_match_audience(user_list_res_name, users, overwrite=True)
    new_user_count, test_user_count, control_user_count = \
        context.data_gateway.update_audience_segment_status(context.target, audience, None, failed_users)
    result[audience_name] = {
      "job_resource_name": job_resource_name,
      "uploaded_user_count": len(uploaded_users),
      "new_user_count": new_user_count,
      "failed_user_count": len(failed_users) if failed_users else 0,
      "test_user_count": test_user_count,
      "control_user_count": control_user_count
    }
    if new_user_count == 0:
      logger.warning(f'Audience segment for {audience_name} for {datetime.now().strftime("%Y-%m-%d")} contains no new users')
    log.append(AudienceLog(
      audience.name,
      datetime.now(),
      job_resource_name,
      len(uploaded_users),
      new_user_count,
      test_user_count,
      control_user_count)
    )

  context.data_gateway.update_audiences_log(context.target, log)
  return jsonify({"result": result})


@app.route("/api/audiences/status", methods=["GET"])
def get_audiences_status():
  context = create_context()
  ads_config = _get_ads_config(context.target)
  ads_client = GoogleAdsApiClient(config_dict=ads_config)
  ads_gateway = AdsGateway(context.config, context.target, ads_client)

  audiences = context.data_gateway.get_audiences(context.target)
  audiences_log = context.data_gateway.get_audiences_log(context.target)
  jobs_status = ads_gateway.get_userlist_jobs_status()
  logger.debug(jobs_status)
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
          "new_user_count": log_item.new_user_count,
          "test_user_count": log_item.test_user_count,
          "control_user_count": log_item.control_user_count
        }
        job_status = next(((j[1], j[2]) for j in jobs_statuses if j[0] == log_item.job), None)
        if not job_status is None:
          status, failure_reason = job_status
        else:
          status, failure_reason = None, None
        log_item_dict['job_status'] = status
        log_item_dict['job_failure'] = failure_reason
        audience_dict['log'].append(log_item_dict)
  return jsonify({"result": result})


@app.route("/api/audiences/conversions", methods=["GET"])
def get_user_conversions():
  context = create_context()

  date_start = request.args.get('date_start')
  date_start = date.fromisoformat(date_start) if date_start else None
  date_end = request.args.get('date_end')
  date_end = date.fromisoformat(date_end) if date_end else None

  audiences = context.data_gateway.get_audiences(context.target)
  audience_name = request.args.get('audience')
  results = {}
  for audience in audiences:
    if audience_name and audience.name != audience_name:
      continue
    result, date_start, date_end = context.data_gateway.get_user_conversions(context.target, audience, date_start, date_end)
    results[audience.name] = result
    if audience_name and audience.name == audience_name:
      break
  return jsonify({
      "results": results,
      "date_start": date_start.strftime("%Y-%m-%d"),
      "date_end": date_end.strftime("%Y-%m-%d")
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
  response.cache_control.no_store = True

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


@app.route("/_ah/start")
def on_instance_start():
  """Instance start handler. It's called by GAE to start a new instance (not workers).

  Be mindful about code here because errors won't be propagated to webapp users
  (i.e. visible only in log)"""
  pass


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
