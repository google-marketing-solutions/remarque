from datetime import datetime
import pandas as pd
import pandas_gbq

from context import Context
from config import Config, ConfigTarget, parse_arguments, get_config, save_config, AppNotInitializedError
from sampling import do_sampling
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
  logger.debug(f"Running sampling for '{audience.name}' audience")
  df = context.data_gateway.fetch_audience_users(context.target, audience)
  logger.info(f"Created a user segment of audience '{audience.name}' with {len(df)} users")
  if len(df) == 0:
    logger.warning("User segment of audience '{audience.name}' contains no users")

  if audience.mode == 'test':
    # exclude old users from test and control groups from today's users
    old_test_users, old_ctrl_users = context.data_gateway.load_old_users(context.target, audience)
    mask = df['user'].isin(old_test_users['user']) | df['user'].isin(old_ctrl_users['user'])
    df_new = df[~mask]
    logger.debug(f"The segment has been adjusted to exclude old users and now contains {len(df_new)} users")
    # now df doesn't contain users from previous days

    # if the segment is empty there's no point in sampling
    if len(df_new) > 0:
      users_test, users_control = do_sampling(df_new)
    else:
      # create empty tables for test and control users so other queries wouldn't fail
      users_test = pd.DataFrame(columns=['user'])
      users_control = pd.DataFrame(columns=['user'])

    # add old test/control users from df to the new test/control groups
    old_test_df = df[df['user'].isin(old_test_users['user'])]
    old_control_df = df[df['user'].isin(old_ctrl_users['user'])]

    # append old users to the new test/control groups
    users_test = pd.concat([users_test, old_test_df], ignore_index=True)
    users_control = pd.concat([users_control, old_control_df], ignore_index=True)
    #users_test = users_test.concat(old_test_df, ignore_index=True)
    #users_control = users_control.concat(old_control_df, ignore_index=True)
    logger.debug(f"The today's test/control groups were updated with previously exposed users: test - {len(users_test)} users, control - {len(users_control)} users")
  elif audience.mode == 'prod':
    # in prod mode all users are like test users (to be uploaded to Ads)
    # we don't need control users actually but for simplicity we'll keep them as empty DF
    users_test = df
    users_control = pd.DataFrame(columns=['user'])

  context.data_gateway.save_sampled_users(context.target, audience, users_test, users_control)
  reloaded_users = context.data_gateway.add_yesterdays_users(context.target, audience)
  # NOTE: number of test users can change because we added users with ttl>1 from yesterday
  if not reloaded_users:
    reloaded_users = users_test['user'].tolist()

  return reloaded_users, users_control['user'].tolist()


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

