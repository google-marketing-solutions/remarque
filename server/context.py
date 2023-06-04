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
from data_gateway import DataGateway
from config import Config, ConfigTarget
from cloud_scheduler_gateway import CloudSchedulerGateway

@dataclass
class ContextOptions:
  pass


class Context:

  def __init__(self, config: Config,
               target: ConfigTarget,
               credentials: credentials.Credentials,
               options: ContextOptions = None):
    self.config = config
    self.target = target
    self.credentials = credentials

    self.data_gateway = DataGateway(config, credentials)
    self.storage_client = storage.Client(project=config.project_id,
                                         credentials=credentials)
    self.cloud_scheduler = CloudSchedulerGateway(config, credentials)
