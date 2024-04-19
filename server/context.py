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

from dataclasses import dataclass
from google.auth import credentials
from google.cloud import storage
from ads_gateway import AdsGateway
from data_gateway import DataGateway
from config import Config, ConfigTarget
from cloud_scheduler_gateway import CloudSchedulerGateway
from gaarf.api_clients import GoogleAdsApiClient


@dataclass
class ContextOptions:
  create_ads_gateway: bool


class Context:

  def _get_ads_config(self, target: ConfigTarget):
    ads_config = {
        'developer_token':
            target.ads_developer_token,
        'client_id':
            target.ads_client_id,
        'client_secret':
            target.ads_client_secret,
        'refresh_token':
            target.ads_refresh_token,
        'login_customer_id':
            str(target.ads_login_customer_id or target.ads_customer_id),
        'customer_id':
            str(target.ads_customer_id or target.ads_login_customer_id),
        'use_proto_plus':
            True
    }
    return ads_config

  def __init__(self,
               config: Config,
               target: ConfigTarget,
               credentials: credentials.Credentials,
               options: ContextOptions = None):
    self.config = config
    self.target = target
    self.credentials = credentials

    self.data_gateway = DataGateway(config, credentials, target)
    self.storage_client = storage.Client(
        project=config.project_id, credentials=credentials)
    self.cloud_scheduler = CloudSchedulerGateway(config, credentials)

    if target and options and options.create_ads_gateway:
      ads_config = self._get_ads_config(target)
      ads_client = GoogleAdsApiClient(config_dict=ads_config, version='v15')
      self.ads_gateway = AdsGateway(config, target, ads_client)
