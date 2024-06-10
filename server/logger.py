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

import os
import logging
import env

LOGGER_NAME = 'Remarque'

logging.basicConfig(
    format='[%(asctime)s][%(name)s][%(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

loglevel = logging.getLevelName(os.getenv('LOG_LEVEL') or 'DEBUG')
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(loglevel)

if env.IS_GAE:
  import google.cloud.logging
  from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging

  client = google.cloud.logging.Client()
  handler = CloudLoggingHandler(client, name=LOGGER_NAME)
  handler.setLevel(loglevel)
  setup_logging(handler)

logging.getLogger('gaarf').setLevel(loglevel)
# disable excessive logging from some components
logging.getLogger('google.ads.googleads.client').setLevel(logging.WARNING)
logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
logging.getLogger('smart_open.gcs').setLevel(logging.WARNING)
logging.getLogger('smart_open.smart_open_lib').setLevel(logging.WARNING)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)
