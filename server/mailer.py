#  Copyright 2023-2025 Google LLC
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
"""Methods for emailing."""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-importing-member, wrong-import-position
from google.appengine.api import mail
from config import Config


def send_email(config: Config, to: str, subject: str, body: str):
  """Send an email using AppEngine API."""
  project_id = config.project_id
  sender = f"no-reply@{project_id}.appspotmail.com"

  message = mail.EmailMessage(sender=sender, subject=subject, to=to, body=body)
  message.send()
