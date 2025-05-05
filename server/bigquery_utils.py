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
"""BigQuery utility methods."""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-importing-member, wrong-import-position
from google.cloud import bigquery
from google.api_core import exceptions
from google.cloud.bigquery.dataset import Dataset
from logger import logger


class CloudBigQueryUtils:
  """This class provides methods to simplify BigQuery API usage."""

  def __init__(self, client: bigquery.Client):
    """Initialise new instance of CloudBigQueryUtils."""
    self.client = client
    self.project_id = self.client.project

  def create_dataset_if_not_exists(self, dataset_id: str,
                                   dataset_location: str) -> Dataset:
    """Creates BigQuery dataset if it doesn't exists.

    Args:
      dataset_id: BigQuery dataset id.
      dataset_location: dataset location
    """
    # Construct a BigQuery client object.
    fully_qualified_dataset_id = f'{self.project_id}.{dataset_id}'
    to_create = False
    if dataset_location == 'europe':
      dataset_location = 'eu'
    try:
      ds: Dataset = self.client.get_dataset(fully_qualified_dataset_id)
      if ds.location and dataset_location and ds.location.lower() != dataset_location.lower() or \
         not ds.location and dataset_location or \
         ds.location and not dataset_location:
        self.client.delete_dataset(fully_qualified_dataset_id, True)
        logger.info(
            f'Existing dataset needs to be recreated due to different location (current: {ds.location}, needed: {dataset_location}).'
        )
        to_create = True
      else:
        logger.info('Dataset %s already exists.', fully_qualified_dataset_id)
    except exceptions.NotFound:
      logger.info('Dataset %s is not found.', fully_qualified_dataset_id)
      to_create = True
    if to_create:
      dataset = bigquery.Dataset(fully_qualified_dataset_id)
      dataset.location = dataset_location
      ds = self.client.create_dataset(dataset)
      logger.info('Dataset %s created.', fully_qualified_dataset_id)
    return ds

  def get_dataset(self, dataset_id: str) -> Dataset:
    fully_qualified_dataset_id = f'{self.project_id}.{dataset_id}'
    try:
      return self.client.get_dataset(fully_qualified_dataset_id)
    except exceptions.NotFound:
      pass

  def get_table_creation_time(self, dataset_id, table_id, table_only=False):
    """Get a table's creation date (or view's).

    Args:
      dataset_id: BigQuery dataset id.
      table_id: BigQuery table id.
      table_only: if True, only table creation time is returned.

    Returns:
      Table creation time.
    """
    try:
      table = self.client.get_table(
          f"{self.project_id}.{dataset_id}.{table_id}",)
    except exceptions.NotFound:
      return None
    if table_only and table.table_type != 'TABLE':
      return None
    creation_time = table.created
    return creation_time
