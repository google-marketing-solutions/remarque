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
  def __init__(self, list_resource_name: str):
    self.query_text = f"""
SELECT
offline_user_data_job.resource_name AS resource_name,
offline_user_data_job.status AS status,
offline_user_data_job.failure_reason AS failure_reason,
offline_user_data_job.customer_match_user_list_metadata.user_list AS user_list
FROM offline_user_data_job"""
    if list_resource_name:
      condition = f"offline_user_data_job.customer_match_user_list_metadata.user_list = '{list_resource_name}'"
    # otherwise return all jobs for customer match lists
    else:
      condition = "offline_user_data_job.type = CUSTOMER_MATCH_USER_LIST"
    self.query_text = self.query_text + '\nWHERE\n' + condition