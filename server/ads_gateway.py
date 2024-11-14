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

from enum import Enum
from typing import Union

from config import Config, ConfigTarget
from gaarf.api_clients import GoogleAdsApiClient
from gaarf.base_query import BaseQuery
from gaarf.query_executor import AdsQueryExecutor, AdsReportFetcher
from google.ads.googleads.errors import GoogleAdsException
from logger import logger
from models import Audience
from queries import OfflineJobQuery, UserListCampaignMetrics, UserListCampaigns

#from google.ads.googleads.client import GoogleAdsClient  # type: ignore



_MEMBERSHIP_LIFESPAN = 10000
_MAX_OPERATIONS_PER_JOB = 100000


class UserListOperation(Enum):
  """ Represents an operation of adding or removing users to an audience list"""
  ADD = 'ADD'
  REMOVE = 'REMOVE'


class AdsGateway:
  """ The main worker class that calls the customer match API
        to update the audience lists in Google Ads.
    """

  def __init__(self, config: Config, target: ConfigTarget,
               googleads_api_client: GoogleAdsApiClient) -> None:
    self.googleads_client = googleads_api_client.client
    self.customer_id = str(target.ads_customer_id)
    self.report_fetcher = AdsReportFetcher(googleads_api_client)
    self.query_executor = AdsQueryExecutor(googleads_api_client)
    self.project_id = config.project_id

  def _execute_query(self,
                     query: BaseQuery,
                     cids: list[str],
                     error_message: str = None):
    try:
      return self.report_fetcher.fetch(query, cids)
    except GoogleAdsException as e:
      for error in e.failure.errors:
        raise ValueError(f'{error_message + ": "}{error.message}') from e

  def create_customer_match_user_lists(
      self, audiences: list[Audience]) -> dict[str, str]:
    """Create user list in Google Ads if needed.

    Identifies which audience lists need to be created in Google Ads and
    creates new Customer Match user lists for them.

    Args:
        audiences: A list of audiences.

    Returns:
        A mapping from audience name to customer match user list resource.
    """
    logger.info('Creating customer match user lists if needed')
    # Get the list audiences that already exist to create the new ones only
    query_text = 'SELECT user_list.name, user_list.resource_name AS user_list_name FROM user_list'
    existing_lists = self.report_fetcher.fetch(query_text,
                                               [self.customer_id]).to_list()
    logger.debug('Existing user lists:\n %s', existing_lists)

    lists_to_add = []
    result = {}
    for audience in audiences:
      # we can't skip audiences with mode=off here (though it looks logical)
      # because audiences in mode=off still can be process on-demand from UI

      # Check if the list name is already in existing_lists
      existing_list = next((existing_list for existing_list in existing_lists
                            if audience.name == existing_list[0]), None)
      if not existing_list:
        lists_to_add.append(audience)
        result[audience.name] = None
      else:
        result[audience.name] = existing_list[1]

    if lists_to_add:
      logger.info('User lists to create:\n %s', lists_to_add)

    # Now we'll create a list of UserListOperation objects for each user list
    user_list_service_client = self.googleads_client.get_service(
        'UserListService')
    user_list_operations = []
    for audience in lists_to_add:
      # Creates the user list operation.
      user_list_op = self.googleads_client.get_type('UserListOperation')

      # Creates the new user list.
      user_list = user_list_op.create
      user_list.name = audience.name
      user_list.description = 'Remarque user list'
      # A string that uniquely identifies a mobile application from which the data was collected.
      # For Android, the ID string is the application's package name
      # (for example, "com.labpixies.colordrips" for "Color Drips" given
      # Google Play link https://play.google.com/store/apps/details?id=com.labpixies.colordrips).
      user_list.crm_based_user_list.app_id = audience.app_id
      user_list.crm_based_user_list.upload_key_type = (
          self.googleads_client.enums.CustomerMatchUploadKeyTypeEnum
          .MOBILE_ADVERTISING_ID)
      user_list.membership_life_span = _MEMBERSHIP_LIFESPAN
      user_list_operations.append(user_list_op)

    # Mutate the user lists to create the new ones
    if user_list_operations:
      try:
        response = user_list_service_client.mutate_user_lists(
            customer_id=self.customer_id, operations=user_list_operations)
        for i in range(0, len(user_list_operations)):
          list_name = user_list_operations[i].create.name
          result[list_name] = response.results[i].resource_name
      except GoogleAdsException as e:
        logger.error('Failed to add new user lists %s: %s', lists_to_add,
                     e.error)

    return result

  def upload_customer_match_audience(self,
                                     user_list_resource_name: str,
                                     users: list[str],
                                     overwrite=True):
    """Creates a job with offline user data job operations.

    A job is created for each audience list.

    Args:
      user_list_resource_name: User list resource name to add/remove user from.
      users: List of user ids  to be added.
      overwrite: To remove previous users from the user list.

    Returns:
      A tuple with a resource name of the newly created offline job
      (which will be later used for getting operation success/failure),
      a list of failed user ids, a list of provided user ids.
    """
    offline_user_data_job_service = self.googleads_client.get_service(
        'OfflineUserDataJobService')
    # Creates a new offline user data job.
    offline_user_data_job = self.googleads_client.get_type('OfflineUserDataJob')
    offline_user_data_job.type_ = (
        self.googleads_client.enums.OfflineUserDataJobTypeEnum
        .CUSTOMER_MATCH_USER_LIST)
    offline_user_data_job.customer_match_user_list_metadata.user_list = user_list_resource_name
    # user consents (mandatory since March 6, 2024)
    # TODO: we can't check real user consents at the moment
    offline_user_data_job.customer_match_user_list_metadata.consent.ad_user_data = (
        self.googleads_client.enums.ConsentStatusEnum.GRANTED)
    offline_user_data_job.customer_match_user_list_metadata.consent.ad_personalization = (
        self.googleads_client.enums.ConsentStatusEnum.GRANTED)

    logger.debug("Creating create_offline_user_data_job for user list '%s'",
                 user_list_resource_name)
    # Issues a request to create an offline user data job.
    create_offline_user_data_job_response = offline_user_data_job_service.create_offline_user_data_job(
        customer_id=self.customer_id, job=offline_user_data_job)

    offline_user_data_job_resource_name = create_offline_user_data_job_response.resource_name

    logger.info(
        "Created an offline user data job with resource name '%s' for user list '%s'.",
        offline_user_data_job_resource_name, user_list_resource_name)
    operations = self._build_offline_user_data_job_operations(users, overwrite)
    logger.debug(
        'Created %s operations (OfflineUserDataJobOperation) with overwrite=%s',
        len(operations), overwrite)

    if len(operations) < _MAX_OPERATIONS_PER_JOB:
      failed_user_ids = self._execute_upload_job(
          offline_user_data_job_service, operations,
          offline_user_data_job_resource_name, users)
    else:
      chunks = [
          operations[i:i + _MAX_OPERATIONS_PER_JOB]
          for i in range(0, len(operations), _MAX_OPERATIONS_PER_JOB)
      ]
      logger.debug('Sending OflineUserDataJobOperations in %s chunks',
                   len(chunks))
      failed_user_ids = None
      for i, chunk in enumerate(chunks):
        logger.debug('Sending %s operations chunk %s', len(chunk), i)
        failed_user_ids_chunk = self._execute_upload_job(
            offline_user_data_job_service, chunk,
            offline_user_data_job_resource_name, users)
        if failed_user_ids_chunk:
          if not failed_user_ids:
            failed_user_ids = []
          failed_user_ids.append(failed_user_ids_chunk)

    # run the offline user data job for executing all added operations.
    if users:
      logger.debug(
          'Sending run_offline_user_data_job request to start uploading users')
      offline_user_data_job_service.run_offline_user_data_job(
          resource_name=offline_user_data_job_resource_name)

      return offline_user_data_job_resource_name, failed_user_ids, users
    return None, failed_user_ids, []

  def _execute_upload_job(self, offline_user_data_job_service, operations,
                          job_resource_name: str, users: list[str]):

    request = self.googleads_client.get_type(
        'AddOfflineUserDataJobOperationsRequest')
    request.resource_name = job_resource_name
    request.operations = operations
    request.enable_partial_failure = True
    #request.enable_warnings = True?

    logger.debug(
        'Sending add_offline_user_data_job_operations request with %s operations',
        len(operations))
    response = offline_user_data_job_service.add_offline_user_data_job_operations(
        request=request)
    failed_user_ids = None
    partial_failure = getattr(response, 'partial_failure_error', None)
    if getattr(partial_failure, 'code', None) != 0:
      failure_idx = []
      error_details = getattr(partial_failure, 'details', [])
      for error_detail in error_details:
        failure_message = self.googleads_client.get_type('GoogleAdsFailure')
        # Retrieve the class definition of the GoogleAdsFailure instance
        failure_object = type(failure_message).deserialize(error_detail.value)

        for error in failure_object.errors:
          logger.error(
              'A partial failure at index %s occurred.\n'
              'Error message: %s\nError code: %s',
              error.location.field_path_elements[0].index, error.message,
              error.error_code)
          # Keep track of failure indices to remove them later
          # from the original list
          failure_idx.append(error.location.field_path_elements[0].index)

      # Remove the id from the user list
      failed_user_ids = []
      for idx in sorted(failure_idx, reverse=True):
        failed_user_ids.append(users[idx])
        del users[idx]
      logger.info('Partial failures occurred during '
                  'adding the following user ids to OfflineUserDataJob:')
      logger.info(failed_user_ids)
    return failed_user_ids

  def _build_offline_user_data_job_operations(self,
                                              users: list[str],
                                              overwrite=True) -> list:
    """Creates a UserData object for each user id.

    The first operation will always be remove_all.

    Args:
      users: List of user mobile device ids.
      overwrite: True to remove all previous user in the audiences
      (using OfflineUserDataJobOperation.remove_all flag)

    Returns:
      A list containing the operations to be performed.
        """
    offline_operation = self.googleads_client.get_type(
        'OfflineUserDataJobOperation')
    if overwrite:
      offline_operation.remove_all = True
    operations = [offline_operation]

    for user_id in users:
      # Create a User Identifier for each device_id
      user_identifier = self.googleads_client.get_type('UserIdentifier')
      user_identifier.mobile_id = user_id

      # Creates a UserData object that represents a member of the user list.
      user_data = self.googleads_client.get_type('UserData')
      user_data.user_identifiers.append(user_identifier)

      offline_operation = self.googleads_client.get_type(
          'OfflineUserDataJobOperation')
      offline_operation.create = user_data

      operations.append(offline_operation)

    return operations

  def get_userlist_jobs_status(self,
                               userlist: Union[str, list] = None) -> list[dict]:
    """Returns a list of jobs statuses.

    Args:
      userlist: a list of user list resource names or
        a resource name of a user list, or None to load all user lists

    Returns:
      a list of dictionaries with fields:
        * resource_name
        * status
        * failure_reason
        * user_list
    """

    report = self._execute_query(
        OfflineJobQuery(userlist), [self.customer_id],
        f'Could not load offline jobs from account {self.customer_id} for '
        f'uploading user lists {userlist}')
    jobs = []
    for item in report:
      job_dict = {
          'resource_name': item.resource_name,
          'status': str(item.status),
          'failure_reason': item.failure_reason,
          'user_list': item.user_list
      }
      jobs.append(job_dict)
    logger.debug('Loaded %s jobs', len(jobs))
    return jobs

  def get_userlist_campaigns(self, userlist: Union[str, list] = None):
    logger.info(
        'Getting campaigns and adgroups targeted user lists for audiences: %s',
        userlist)
    cids = self.query_executor.expand_mcc(self.customer_id)
    logger.debug('CID %s expanded to list of customers: %s', self.customer_id,
                 cids)
    report = self.report_fetcher.fetch(UserListCampaigns(userlist), cids)
    ocid_report = self.report_fetcher.fetch(
        'SELECT * FROM builtin.ocid_mapping', cids)
    ocid_mapping = ocid_report.to_dict('account_id', 'ocid', 'scalar')
    for row in report:
      row['ocid'] = ocid_mapping.get(row.customer_id)
    logger.debug(report)
    return report

  def get_userlist_campaigns_metrics(self, cid, campaigns: list[str | int],
                                     date_start, date_end):
    query = UserListCampaignMetrics(campaigns, date_start, date_end)
    report = self.report_fetcher.fetch(query, cid)
    return report
