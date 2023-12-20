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

from typing import Union
from gaarf.base_query import BaseQuery

class UserListQuery(BaseQuery):
  def __init__(self, list_name: str) -> None:
    self.list_name = list_name
    self.query_text = f"""
      SELECT
          user_list.id as id,
          user_list.name as name,
          user_list.resource_name as resource_name
      FROM user_list
      WHERE user_list.name = '{self.list_name}'
    """

class OfflineJobQuery(BaseQuery):
  """Query offline jobs of uploading customer match user lists.
    Returns a list of dicts::
    * resource_name
    * status
    * failure_reason
    * user_list
  """
  def __init__(self, list_name: Union[str,list]):
    self.query_text = f"""
SELECT
  offline_user_data_job.resource_name AS resource_name,
  offline_user_data_job.status AS status,
  offline_user_data_job.failure_reason AS failure_reason,
  offline_user_data_job.customer_match_user_list_metadata.user_list AS user_list
FROM offline_user_data_job"""
    if list_name and isinstance(list_name, str):
      condition = f"offline_user_data_job.customer_match_user_list_metadata.user_list = '{list_name}'"
    # otherwise return all jobs for customer match lists
    elif list_name and isinstance(list_name, list):
      parts = [f"'{i}'" for i in list_name]
      condition = f"offline_user_data_job.customer_match_user_list_metadata.user_list IN ({','.join(parts)})"
    else:
      condition = "offline_user_data_job.type = CUSTOMER_MATCH_USER_LIST"
    self.query_text = self.query_text + '\nWHERE\n' + condition


class UserListCampaigns(BaseQuery):
  def __init__(self, list_name: Union[str,list]):
    self.query_text = f"""
SELECT
  ad_group_criterion.user_list.user_list as user_list,
  customer.id,
  customer.descriptive_name as customer_name,
  campaign.id,
  campaign.name,
  campaign.status,
  campaign.start_date,
  campaign.end_date,
  ad_group.id,
  ad_group.name,
  ad_group.status,
  user_list.name,
  user_list.description
FROM ad_group_criterion
"""
    if list_name and isinstance(list_name, str):
      condition = f"user_list.name = '{list_name}'"
    # otherwise return all jobs for customer match lists
    elif list_name and isinstance(list_name, list):
      parts = [f"'{i}'" for i in list_name]
      condition = f"user_list.name IN ({','.join(parts)})"
    else:
      condition = "ad_group_criterion.type = USER_LIST AND user_list.description = 'Remarque user list' AND ad_group_criterion.status = ENABLED"
    self.query_text = self.query_text + '\nWHERE\n' + condition

