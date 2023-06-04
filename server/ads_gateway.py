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
from google.ads.googleads.errors import GoogleAdsException
#from google.ads.googleads.client import GoogleAdsClient  # type: ignore
import smart_open
from yaml.loader import SafeLoader

from gaarf.api_clients import GoogleAdsApiClient
from gaarf.query_executor import AdsReportFetcher

from logger import logger
from config import Config, ConfigTarget, Audience
from queries import UserListQuery, OfflineJobQuery

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
        self.report_fetcher = AdsReportFetcher(
            googleads_api_client,
            [self.customer_id])

        self.project_id = config.project_id


    def create_customer_match_user_lists(self, audiences: list[Audience])-> dict[str,str]:
        """ Identifies which audience lists need to be created in Google Ads and
            and creates new Customer Match user lists with the Google Ads API for them.

        Args:
            audiences: list of audiences
        Returns:
            dict: a mapping from audience name to customer match user list resource
        """
        logger.info("Creating customer match user lists")
        # Get the list audiences that already exist to create the new ones only
        query_text = "SELECT user_list.name, user_list.resource_name AS user_list_name FROM user_list"
        existing_lists = self.report_fetcher.fetch(query_text).to_list()
        logger.debug('Existing user lists: ')
        logger.debug(existing_lists)

        lists_to_add = []
        result = {}
        for audience in audiences:
          if not audience.active:
            logger.debug(f"Skipping non-active audience {audience.name}")
            continue

          # Check if the list name is already in existing_lists
          existing_list = next(
              (existing_list for existing_list in existing_lists if audience.name == existing_list[0]), None)
          if not existing_list:
            lists_to_add.append(audience)
            result[audience.name] = None
          else:
            result[audience.name] = existing_list[1]

        logger.info('User lists to create: ')
        logger.info(lists_to_add)

        # Now we'll create a list of UserListOperation objects for each user list
        user_list_service_client = self.googleads_client.get_service("UserListService")
        user_list_operations = []
        for audience in lists_to_add:
          # Creates the user list operation.
          user_list_op= self.googleads_client.get_type("UserListOperation")

          # Creates the new user list.
          user_list = user_list_op.create
          user_list.name = audience.name
          user_list.description = "Remarque user list"
          # A string that uniquely identifies a mobile application from which the data was collected.
          # For iOS, the ID string is the 9 digit string that appears at the end of an App Store URL (for example, "476943146" for "Flood-It! 2" whose App Store link is http://itunes.apple.com/us/app/flood-it!-2/id476943146).
          # For Android, the ID string is the application's package name (for example, "com.labpixies.colordrips" for "Color Drips" given Google Play link https://play.google.com/store/apps/details?id=com.labpixies.colordrips).
          user_list.crm_based_user_list.app_id = audience.app_id
          user_list.crm_based_user_list.upload_key_type = self.googleads_client.enums.CustomerMatchUploadKeyTypeEnum.MOBILE_ADVERTISING_ID
          user_list.membership_life_span = _MEMBERSHIP_LIFESPAN
          user_list_operations.append(user_list_op)

        # Mutate the user lists to create the new ones
        if user_list_operations:
          try:
            response = user_list_service_client.mutate_user_lists(
              customer_id=self.customer_id, operations=user_list_operations
            )
            for i in range(0, len(user_list_operations)):
              list_name = user_list_operations[i].create.name
              result[list_name] = response.results[i].resource_name
          except GoogleAdsException as e:
            logger.error(f"Failed to add new user lists {lists_to_add}: {e.error}")

        return result


    def upload_customer_match_audience(self, list_resource_name: str, users: list[str], overwrite=True):
      if len(users) > _MAX_OPERATIONS_PER_JOB:
          raise ValueError(f'Too many users ({len(users)}) in the list to upload at once (maximum: {_MAX_OPERATIONS_PER_JOB})')
      # Create an offline job with all users identities
      offline_job_resource_name, failed_user_ids, users = self._create_and_run_offline_user_data_job(
          list_resource_name,
          users,
          overwrite
      )
      return offline_job_resource_name, failed_user_ids, users


    def _create_and_run_offline_user_data_job(self,
                                              user_list_resource_name: str,
                                              users: list[str],
                                              overwrite: True):
        """ Creates a offline user data job operation for an audience list and runs it.

            Args:
                user_list_resource_name: user list resource name to add/remove user from
                user_emails: List of user emails to be added/removed

            Returns:
                The resource name of the newly created offline job which will be later
                used for getting operation success/failure
        """
        offline_user_data_job_service = self.googleads_client.get_service(
            "OfflineUserDataJobService"
        )
        # Creates a new offline user data job.
        offline_user_data_job =self.googleads_client.get_type("OfflineUserDataJob")
        offline_user_data_job.type_ = self.googleads_client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
        offline_user_data_job.customer_match_user_list_metadata.user_list = user_list_resource_name

        # Issues a request to create an offline user data job.
        create_offline_user_data_job_response = offline_user_data_job_service.create_offline_user_data_job(
            customer_id=self.customer_id, job=offline_user_data_job
        )

        offline_user_data_job_resource_name = create_offline_user_data_job_response.resource_name

        logger.info(
            f"Created an offline user data job with resource name '{offline_user_data_job_resource_name}' for user list '{user_list_resource_name}'."
        )
        request = self.googleads_client.get_type("AddOfflineUserDataJobOperationsRequest")
        request.resource_name = offline_user_data_job_resource_name
        # NOTE: AddOfflineUserDataJobOperationsRequest.operations is limited to 100,000 elements maximum
        request.operations = self._build_offline_user_data_job_operations(users, overwrite)
        request.enable_partial_failure = True
        #request.enable_warnings = True?
        #print(request.operations)

        # Issues a request to add the operations to the offline user data job.
        response = offline_user_data_job_service.add_offline_user_data_job_operations(
            request=request
        )

        # Extracts the partial failure from the response status.
        failed_user_ids = None
        partial_failure = getattr(response, "partial_failure_error", None)
        if getattr(partial_failure, "code", None) != 0:
            failure_idx = []
            error_details = getattr(partial_failure, "details", [])
            for error_detail in error_details:
                failure_message = self.googleads_client.get_type("GoogleAdsFailure")
                # Retrieve the class definition of the GoogleAdsFailure instance
                failure_object = type(failure_message).deserialize(
                    error_detail.value
                )

                for error in failure_object.errors:
                    logger.error(
                        "A partial failure at index "
                        f"{error.location.field_path_elements[0].index} occurred.\n"
                        f"Error message: {error.message}\n"
                        f"Error code: {error.error_code}"
                    )
                    # Keep track of failure indeces to remove them later from the original list
                    failure_idx.append(error.location.field_path_elements[0].index)

            # Remove the id from the user list
            failed_user_ids = []
            for idx in sorted(failure_idx, reverse=True):
                failed_user_ids.append(users[idx])
                del users[idx]
            logger.info('Partical failures occured during adding the following user ids to OfflineUserDataJob:')
            logger.info(failed_user_ids)

        # Issues a request to run the offline user data job for executing all
        # added operations.
        if users:
            offline_user_data_job_service.run_offline_user_data_job(
                resource_name=offline_user_data_job_resource_name
            )

            return offline_user_data_job_resource_name, failed_user_ids, users
        return None, failed_user_ids, []


    def _build_offline_user_data_job_operations(self,
                                                users: list[str],
                                                overwrite = True) -> list:
        """Creates a UserData object for each user id. The first operation will always be remove_all

        Args:
          overwrite: True to remove all previous user in the audiences
            (using OfflineUserDataJobOperation.remove_all flag)
          users: List of user mobile device ids

        Returns:
          A list containing the operations to be performed.
        """
        offline_operation = self.googleads_client.get_type("OfflineUserDataJobOperation")
        if overwrite:
          offline_operation.remove_all = True
        operations = [offline_operation]

        for user_id in users:
            # Create a User Identifier for each device_id
            user_identifier = self.googleads_client.get_type("UserIdentifier")
            user_identifier.mobile_id = user_id

            # Creates a UserData object that represents a member of the user list.
            user_data = self.googleads_client.get_type("UserData")
            user_data.user_identifiers.append(user_identifier)

            offline_operation = self.googleads_client.get_type("OfflineUserDataJobOperation")
            offline_operation.create = user_data

            operations.append(offline_operation)

        return operations


    def get_userlist_jobs_status(self, userlist_resource_name = None) -> list[dict]:
      """Returns a list of jobs statuses with the columns:
          * resource_name
          * status
          * failure_reason
          * user_list
      """
      report = self.report_fetcher.fetch(OfflineJobQuery(userlist_resource_name))
      jobs = []
      for item in report:
          job_dict = {
              "resource_name": item.resource_name,
              "status": str(item.status),
              "failure_reason": item.failure_reason,
              "user_list": item.user_list
          }
          jobs.append(job_dict)
      return jobs
