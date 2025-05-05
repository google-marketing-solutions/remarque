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
"""Common global variables."""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-importing-member, wrong-import-position
import os


def get_val(envvar):
  if envvar == 0:
    return envvar
  if not envvar or envvar == 'None':
    return ''
  return envvar


IS_GAE = bool(get_val(os.getenv('GAE_APPLICATION')))
"""True if the app is inside AppEngine"""

GAE_LOCATION = os.getenv('GAE_LOCATION')

SERVICE_ACCOUNT = os.getenv(
    'GOOGLE_CLOUD_PROJECT') + '@appspot.gserviceaccount.com' if IS_GAE else ''


def get_temp_dir():
  if IS_GAE:
    return '/tmp'
  return '.tmp'
