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

import argparse
import google.auth
from google.auth import credentials

def get_credentials(args: argparse.Namespace=None) -> credentials.Credentials:
  credentials, _ = google.auth.default()

  # we can support running the app without default application credentials as above with an explicit key file:
  # from google.oauth2 import service_account  # type: ignore
  # from google_auth_oauthlib import flow
  # if args and args["service_account_file"]:
  #   try:
  # _SCOPES = [
  #     'https://www.googleapis.com/auth/cloud-platform',
  #     'https://www.googleapis.com/auth/bigquery',
  #     'https://www.googleapis.com/auth/spreadsheets',
  # ]
  #     credentials = service_account.Credentials.from_service_account_file(
  #         args.service_account_file, scopes=scopes)
  #   except ValueError as e:
  #     raise Exception(
  #         "Invalid json file for service account authenication") from e
  # else:
  #   # NOTE: if you use `gcloud auth application-default login` then the scopes here will be ignored,
  #   #       you should specify them as parameter --scopes for the gcloud command
  #   credentials, project = google.auth.default(scopes=scopes)

  return credentials
