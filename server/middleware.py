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
from datetime import datetime
import pandas as pd
import pandas_gbq

from context import Context
from sampling import split_via_stratification
from models import Audience, AudienceLog
from logger import logger


def run_sampling_for_audience(context: Context, audience: Audience) -> tuple[list[str], list[str]] | None:
  """
  Samples an audience (assuming it's in test or prod mode):
    - fetch users according to the audience definition
    - do sampling, i.e. split users onto test and control groups (for prod mode the control group will be empty)
  Returns:
    - a tuple of two DataFrames with test and control users
  """
  if audience.mode == 'off':
    return
  logger.debug(f"Starting sampling for '{audience.name}' audience")

  if audience.mode == 'test':
    df = context.data_gateway.sample_audience_users(context.target, audience, None, return_only_new_users=True)
    # df contains new users with all features used by splitting algorithm (brand, osv, days_since_install, src, n_sessions),
    # i.e. it doesn't contain users from today's segment that got into audience on any previous day (we'll load them later)
    if len(df) > 0:
      logger.debug(f"Starting splitting users of audience '{audience.name}' with {audience.split_ratio if audience.split_ratio else 'default'} ratio")
      # now split users in df into two groups, treatment and control using stratification by the audience's ration (default 0.5)
      users_test, users_control = split_via_stratification(df, audience.split_ratio)
    else:
      logger.warning(f"User segment of audience '{audience.name}' contains no users")
      # create empty tables for test and control users so other queries won't fail
      users_test = pd.DataFrame(columns=['user'])
      users_control = pd.DataFrame(columns=['user'])
    # `users_test` and `users_control` are DataFrames with test and control users accordingly from the `df` DataFrame but contain only 'user' column
  elif audience.mode == 'prod':
    # in prod mode all users are like test users (to be uploaded to Ads)
    # we don't need control users actually but for simplicity we'll keep them as empty DF
    df = context.data_gateway.sample_audience_users(context.target, audience, None, return_only_new_users=False)
    users_test = df
    users_control = pd.DataFrame(columns=['user'])

  context.data_gateway.save_sampled_users(context.target, audience, users_test, users_control)
  if audience.mode == 'test':
    # now add users captured by the audience (i.e. are contained in the todays's segment) but that existed on previous days ("old" users)
    context.data_gateway.add_previous_sampled_users(context.target, audience)

  # now add test users from yesterday with ttl>1
  context.data_gateway.add_yesterdays_users(context.target, audience)

  # finally load resulting sets of test and control users for today (actually for suffix)
  test_users = context.data_gateway.load_audience_segment(context.target, audience, 'test')

  return test_users, users_control['user'].tolist()


def update_customer_match_mappings(context: Context, audiences: list[Audience]):
  user_lists = context.ads_gateway.create_customer_match_user_lists(audiences)
  logger.debug(user_lists)
  # update user list resource names for audiences
  need_updating = False
  for list_name, res_name in user_lists.items():
    for audience in audiences:
      if audience.name == list_name:
        if not audience.user_list:
          need_updating = True
        elif audience.user_list != res_name:
          logger.error(f'An audience {list_name} already has user_list ({audience.user_list}) but not the expected one - {res_name}')
          need_updating = True
        audience.user_list = res_name
  if need_updating:
    context.data_gateway.update_audiences(context.target, audiences)


def upload_customer_match_audience(context: Context,
                                   audience: Audience, audience_log: list[AudienceLog],
                                   users: list[str] = None):
  if audience.mode == 'off':
    return

  audience_name = audience.name
  user_list_res_name = audience.user_list
  # load of users for 'today' table (audience_{listname}_test_yyyyMMdd)
  if not users:
    users = context.data_gateway.load_audience_segment(context.target, audience, 'test')

  # upload users to Google Ads
  if len(users) > 0:
    job_resource_name, failed_users, uploaded_users = \
        context.ads_gateway.upload_customer_match_audience(user_list_res_name, users, True)
    # update audience status in our tables, plus calculate some statistics
    context.data_gateway.update_audience_segment_status(context.target, audience, None, failed_users)
    test_user_count, control_user_count, new_test_user_count, new_control_user_count = \
      context.data_gateway.load_user_segment_stat(context.target, audience, None)
    logger.info(f"Newly uploaded segment contains new {new_test_user_count} test users (of {test_user_count}) and new {new_control_user_count} control users (of {control_user_count})")
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
  # TODO: add some flag to control the behavior
  if len(uploaded_users):
    uploaded_users_mapped = [[id] for id in uploaded_users]
    df = pd.DataFrame(uploaded_users_mapped, columns=['user'])
    uploaded_table_name = context.data_gateway._get_user_segment_table_full_name(context.target, audience.table_name, 'uploaded')
    pandas_gbq.to_gbq(df[['user']], uploaded_table_name, context.config.project_id, if_exists='replace')

  # now calculate total numbers of users in the audience
  total_test_user_count = 0
  total_control_user_count = 0
  # get total_user_count from the previous log entry if it exists and add new_user_count
  if not audience_log:
    total_test_user_count = new_test_user_count
    total_control_user_count = new_control_user_count
  else:
    # take a AudienceLog entry for the previous day (not for today!)
    today = datetime.now().strftime("%Y-%m-%d")
    audience_log.sort(key=lambda i: i.date, reverse=True)
    previous_day_log = next((obj for obj in audience_log if obj.date.strftime("%Y-%m-%d") != today), None)
    if previous_day_log:
      total_test_user_count = previous_day_log.total_test_user_count + new_test_user_count
      total_control_user_count = previous_day_log.total_control_user_count + new_control_user_count
    else:
      total_test_user_count = new_test_user_count
      total_control_user_count = new_control_user_count
  if new_test_user_count == 0:
    logger.warning(f'Audience segment for {audience_name} for {datetime.now().strftime("%Y-%m-%d")} contains no new users')

  return AudienceLog(
    name=audience.name,
    date=datetime.now(),
    job_resource_name=job_resource_name,
    uploaded_user_count=len(uploaded_users),
    new_test_user_count=new_test_user_count,
    new_control_user_count=new_control_user_count,
    test_user_count=test_user_count,
    control_user_count=control_user_count,
    total_test_user_count=total_test_user_count,
    total_control_user_count=total_control_user_count,
    failed_user_count=len(failed_users) if failed_users else 0,
  )
