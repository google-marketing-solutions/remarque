#  Copyright 2023-2005 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Middleware methods."""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-importing-member, wrong-import-position
from datetime import datetime
import pandas as pd
import pandas_gbq

from context import Context
from sampling import split_via_stratification
from models import Audience, AudienceLog, SplittingResult
from logger import logger


def run_sampling_for_audience(
    context: Context,
    audience: Audience,
    suffix: str | None = None
) -> tuple[list[str], list[str], SplittingResult | None] | None:
  """Sample an audience (assuming it's in test or prod mode).

  Sampling includes:
    - fetch users according to the audience definition
    - do sampling, i.e. split users onto test and control groups
      (for prod mode the control group will be empty)

  Args:
    context: A server call context.
    audience: An audience to process.
    suffix: optional suffix to choose a day, by default yesterday

  Returns:
    - a tuple of two DataFrames with test and control users
  """
  if audience.mode == 'off':
    return
  logger.debug("Starting sampling for '%s' audience (mode=%s)", audience.name,
               audience.mode)
  split_result: SplittingResult = None
  if audience.mode == 'test':
    sampled_users_new = context.data_gateway.sample_audience_users(
        context.target, audience, suffix, return_only_new_users=True)
    # df contains new users with all features used by splitting algorithm
    # (brand, osv, days_since_install, src, n_sessions).
    # i.e. it doesn't contain users from today's segment that got into audience
    # on any previous day (we'll load them later)
    if not sampled_users_new.empty:
      logger.debug(
          "Starting splitting users (%s) of audience '%s' "
          'via stratification with %s ratio', len(sampled_users_new),
          audience.name,
          audience.split_ratio if audience.split_ratio else 'default')
      # now split users in df into two groups (treatment and control)
      # using stratification in the audience's ratio (default 0.5)
      split_result = split_via_stratification(sampled_users_new,
                                              audience.split_ratio)
      users_new_test = split_result.users_test
      users_new_control = split_result.users_control
      logger.debug(
          "Splitting users of audience '%s' has completed: "
          'test count = %s, control count = %s', audience.name,
          len(users_new_test), len(users_new_control))
      context.data_gateway.save_split_statistics(context.target, audience,
                                                 split_result, suffix)
    else:
      logger.warning("User segment of audience '%s' contains no users",
                     audience.name)
      # create empty tables for test/control users so other queries won't fail
      users_new_test = pd.DataFrame(columns=['user'])
      users_new_control = pd.DataFrame(columns=['user'])
    # users_test and users_control are DataFrames with test and control users
    # accordingly from the `df` DataFrame but contain only 'user' column
  elif audience.mode == 'prod':
    # in prod mode all users are like test users (to be uploaded to Ads)
    # we don't need control users actually
    # but for simplicity we'll keep them as empty DF
    sampled_users_new = context.data_gateway.sample_audience_users(
        context.target, audience, suffix, return_only_new_users=False)
    users_new_test = sampled_users_new
    users_new_control = pd.DataFrame(columns=['user'])

  context.data_gateway.save_sampled_users(context.target, audience,
                                          users_new_test, users_new_control,
                                          suffix)
  if audience.mode == 'test':
    # now add users captured by the audience (i.e. are contained in
    # the todays's segment) but that existed on previous days ("old" users)
    context.data_gateway.add_previous_sampled_users(context.target, audience,
                                                    suffix)

  # now add users from yesterday with ttl>1
  context.data_gateway.add_yesterdays_users(context.target, audience)

  # finally load resulting sets of test and control users for today
  test_users = context.data_gateway.load_audience_segment(
      context.target, audience, 'test', suffix)
  control_users = context.data_gateway.load_audience_segment(
      context.target, audience, 'control', suffix)

  return test_users, control_users, split_result


def update_customer_match_mappings(context: Context, audiences: list[Audience]):
  """Creates Customer Match user lists in Google Ads and updates audience objects.

  This function iterates through the provided audiences, checks if they have
  an associated user list in Google Ads, creates one if it doesn't exist,
  and updates the audience's `user_list` attribute with the resource name.
  If any audience's `user_list` needs updating, it saves the changes back
  to the data store.

  Args:
    context: The application context.
    audiences: A list of Audience objects to process.
  """
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
          logger.error(
              'An audience %s already has user_list (%s) '
              'but not the expected one - %s', list_name, audience.user_list,
              res_name)
          need_updating = True
        audience.user_list = res_name
  if need_updating:
    context.data_gateway.update_audiences(context.target, audiences)


def upload_customer_match_audience(context: Context,
                                   audience: Audience,
                                   audience_log: list[AudienceLog],
                                   users: list[str] = None):
  """Uploads users to a Google Ads Customer Match audience list.

  Loads the users for the specified audience (either provided or loaded from
  the data store), uploads them to the corresponding Google Ads user list,
  updates the audience status, calculates statistics, and logs the operation.

  Args:
    context: The application context.
    audience: The Audience object representing the target audience.
    audience_log: A list of previous AudienceLog entries for this audience.
    users: An optional list of user IDs to upload. If None, users are loaded
           from the data store for the current day's 'test' segment.

  Returns:
    An AudienceLog object containing details and statistics about the upload
    operation. Returns None if the audience mode is 'off'.
  """
  if audience.mode == 'off':
    return

  audience_name = audience.name
  user_list_res_name = audience.user_list
  # load of users for 'today' table (audience_{listname}_test_yyyyMMdd)
  if not users:
    users = context.data_gateway.load_audience_segment(context.target, audience,
                                                       'test')

  # upload users to Google Ads
  if len(users) > 0:
    logger.info('Starting uploading user ids (%s) to audience %s user list',
                len(users), audience.name)
    job_resource_name, failed_users, uploaded_users = (
        context.ads_gateway.upload_customer_match_audience(
            user_list_res_name, users, True))
    # update audience status in our tables, plus calculate some statistics
    context.data_gateway.update_audience_segment_status(context.target,
                                                        audience, None,
                                                        failed_users)
    (test_user_count, control_user_count, new_test_user_count,
     new_control_user_count) = (
         context.data_gateway.load_user_segment_stat(context.target, audience,
                                                     None))
    logger.info(
        'Newly uploaded segment contains new %s test users (of %s) '
        'and new %s control users (of %s)', new_test_user_count,
        test_user_count, new_control_user_count, control_user_count)
  else:
    logger.warning("Audience '%s' segment has no users for today",
                   audience.name)
    job_resource_name = None
    failed_users = []
    uploaded_users = []
    test_user_count = 0
    control_user_count = 0
    new_test_user_count = 0
    new_control_user_count = 0

  # for debug reason save uploaded users
  # TODO: add some flag to control the behavior
  if len(uploaded_users):
    uploaded_users_mapped = [[id] for id in uploaded_users]
    df = pd.DataFrame(uploaded_users_mapped, columns=['user'])
    uploaded_table_name = context.data_gateway.get_user_segment_table_full_name(
        context.target, audience.table_name, 'uploaded')
    pandas_gbq.to_gbq(
        df[['user']],
        uploaded_table_name,
        context.config.project_id,
        if_exists='replace')

  # now calculate total numbers of users in the audience
  total_test_user_count = 0
  total_control_user_count = 0
  # get total_user_count from the previous log entry if it exists
  # and add new_user_count
  if not audience_log:
    total_test_user_count = new_test_user_count
    total_control_user_count = new_control_user_count
  else:
    # take a AudienceLog entry for the previous day (not for today!)
    today = datetime.now().strftime('%Y-%m-%d')
    audience_log.sort(key=lambda i: i.date, reverse=True)
    previous_day_log = next(
        (obj for obj in audience_log if obj.date.strftime('%Y-%m-%d') != today),
        None)
    if previous_day_log:
      total_test_user_count = (
          previous_day_log.total_test_user_count + new_test_user_count)
      total_control_user_count = (
          previous_day_log.total_control_user_count + new_control_user_count)
    else:
      total_test_user_count = new_test_user_count
      total_control_user_count = new_control_user_count
  if new_test_user_count == 0:
    logger.warning('Audience segment for %s for %s contains no new users',
                   audience_name,
                   datetime.now().strftime('%Y-%m-%d'))

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
