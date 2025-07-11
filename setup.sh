#!/bin/bash

# Copyright 2023-2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

COLOR='\033[0;36m' # Cyan
NC='\033[0m'       # No Color
RED='\033[0;31m'   # Red (error)

SETTING_FILE="./settings.ini"
SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)
SETTING_FILE="${SCRIPT_PATH}/settings.ini"

# changing the cwd to the script's contining folder so all pathes inside can be local to it
# (important as the script can be called via absolute path and as a nested path)
pushd $SCRIPT_PATH > /dev/null

while :; do
    case $1 in
  -s|--settings)
      shift
      SETTING_FILE=$1
      ;;
  *)
      break
    esac
  shift
done

PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="csv(projectNumber)" | tail -n 1)
SERVICE_ACCOUNT=$PROJECT_ID@appspot.gserviceaccount.com

echo "Service account: $SERVICE_ACCOUNT"

check_billing() {
  BILLING_ENABLED=$(gcloud beta billing projects describe $PROJECT_ID --format="csv(billingEnabled)" | tail -n 1)
  if [[ "$BILLING_ENABLED" = 'False' ]]
  then
    echo -e "${RED}The project $PROJECT_ID does not have a billing enabled. Please activate billing${NC}"
    exit -1
  fi
}

enable_apis() {
  echo -e "${COLOR}Enabling APIs${NC}"

  gcloud services enable appengine.googleapis.com
  gcloud services enable bigquery.googleapis.com
  gcloud services enable iamcredentials.googleapis.com
  gcloud services enable googleads.googleapis.com
  gcloud services enable cloudscheduler.googleapis.com
  gcloud services enable iap.googleapis.com
  gcloud services enable cloudresourcemanager.googleapis.com

  gcloud services enable artifactregistry.googleapis.com # required for Gen2 GCF
#  gcloud services enable cloudbuild.googleapis.com
}

create_gae() {
  # NOTE: despite other GCP services GAE supports only two regions: europe-west and us-central
  local GAE_REGION=$(git config -f $SETTING_FILE gae.region)
  gcloud app describe > /dev/null 2> /dev/null
  APP_EXISTS=$?
  if [[ $APP_EXISTS -ne 0 ]]; then
    echo -e "${COLOR}Creating AppEngine application${NC}"
    gcloud app create --region $GAE_REGION
  fi
}

set_iam_permissions() {
  SERVICE_ACCOUNT=$(gcloud app describe --format="value(serviceAccount)")
  echo -e "${COLOR}Setting up IAM permissions...${NC}"
  declare -ar ROLES=(
    roles/appengine.appAdmin
    roles/bigquery.admin
    roles/cloudscheduler.admin
    roles/storage.admin
    roles/logging.logWriter
    roles/logging.admin
    roles/iap.admin
    roles/artifactregistry.admin
    roles/iam.serviceAccountUser
  )
  for role in "${ROLES[@]}"
  do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member=serviceAccount:$SERVICE_ACCOUNT \
      --role=$role \
      --condition=None
  done
}

update_git_commit() {
  echo -e "${COLOR}Updating last GIT commit in app.yaml...${NC}"
  GIT_COMMIT=$(git rev-parse HEAD)
  sed -i'.original' -e "s/GIT_COMMIT\s*:\s*.*$/GIT_COMMIT: $GIT_COMMIT/" app.yaml
  # TODO: use it in code
  #  commit = os.getenv('GIT_COMMIT') or ''

  #TODO:
  #CONFIG: gs://$PROJECT_ID/remarque/config.json
}


deploy_files() {
  echo -e "${COLOR}Deploying files to GCS...${NC}"
  if ! gsutil ls gs://${PROJECT_ID} > /dev/null 2> /dev/null; then
    gsutil mb -b on gs://${PROJECT_ID}
  fi

  GCS_BUCKET=gs://${PROJECT_ID}/remarque
  gsutil cp app.yaml $GCS_BUCKET/
  #gsutil cp config.json $GCS_BUCKET/
  echo -e "${COLOR}Files were deployed to ${GCS_BUCKET}${NC}"
}


build_app() {
  echo -e "${COLOR}Building app...${NC}"
  npm i
  npm run build
}

clean_dispatch_rules() {
  echo -e "${COLOR}Cleaning dispatch rules...${NC}"

  # Create empty dispatch file
  echo "dispatch:" > dispatch.yaml

  # Deploy it
  gcloud app deploy dispatch.yaml --quiet

  # Clean up
  rm dispatch.yaml
}

deploy_app() {
  build_app
  clean_dispatch_rules
  echo -e "${COLOR}Deploying app to GAE...${NC}"
  gcloud app deploy --quiet
}


create_iap() {
  local USER_EMAIL=$(gcloud config get-value account 2> /dev/null)
  local PROJECT_TITLE=Remarque

  # create IAP
  IAP_BRAND=$(gcloud iap oauth-brands list --format='value(name)' 2>/dev/null)
  # creating oauth-brand will fail in projects outside an Organization
  exit_status=$?
  if [ $exit_status -ne 0 ]; then
    echo -e "${RED}OAuth brand (a.k.a. OAuth consent screen) failed to create, please create it manualy in Cloud Console:\n${NC}https://console.cloud.google.com/apis/credentials/consent"
    return -1
  fi

  if [[ ! -n $IAP_BRAND ]]; then
    # IAP OAuth brand doesn't exists, creating
    echo -e "${COLOR}Creating oauth brand (consent screen) for IAP...${NC}"
    IAP_BRAND=projects/$PROJECT_NUMBER/brands/$PROJECT_NUMBER
    USER_EMAIL=$(gcloud config get-value account 2> /dev/null)
    gcloud iap oauth-brands create --application_title="$PROJECT_TITLE" --support_email=$USER_EMAIL
  else
    echo -e "${COLOR}Found an IAP oauth brand (consent screen):'${IAP_BRAND}'${NC}"
  fi
  # name: projects/xxx/brands/yyy

  # Find or create OAuth client for IAP
  output=$(gcloud iap oauth-clients list $IAP_BRAND --format='csv(name,secret)' 2>/dev/null | tail -n +2)

  IAP_CLIENT_ID=
  IAP_CLIENT_SECRET=
  while IFS=',' read -r name secret; do
    if [[ -n "$name" && -n "$secret" ]]; then
      IAP_CLIENT_ID=$name
      IAP_CLIENT_SECRET=$secret
      break
    fi
  done <<< "$output"

  if [[ ! -n $IAP_CLIENT_ID ]]; then
    echo -e "${COLOR}Creating OAuth client for IAP ($IAP_BRAND)...${NC}"
    output=$(gcloud iap oauth-clients create $IAP_BRAND --display_name=iap --format='csv(name,secret)' | tail -n +2)
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
      echo "Command failed with exit status $exit_status"
      return -1
    fi
    while IFS=',' read -r name secret; do
      if [[ -n "$name" && -n "$secret" ]]; then
        IAP_CLIENT_ID=$name
        IAP_CLIENT_SECRET=$secret
        break
      fi
    done <<< "$output"
    echo -e "${COLOR}Created IAP OAuth client: ${IAP_CLIENT_ID}${NC}"
  else
    echo -e "${COLOR}Found IAP OAuth client ${IAP_CLIENT_ID}${NC}"
  fi
  IAP_CLIENT_ID=$(basename "${IAP_CLIENT_ID}")

  TOKEN=$(gcloud auth print-access-token)

  # Enable IAP for AppEngine
  # (source:
  #   https://cloud.google.com/iap/docs/managing-access#managing_access_with_the_api
  #   https://cloud.google.com/iap/docs/reference/app-engine-apis)
  echo -e "${COLOR}Enabling IAP for App Engine...${NC}"
#  curl -X PATCH -H "Content-Type: application/json" \
#   -H "Authorization: Bearer $TOKEN" \
#   --data "{\"iap\": {\"enabled\": true, \"oauth2ClientId\": \"$IAP_CLIENT_ID\", \"oauth2ClientSecret\": \"$IAP_CLIENT_SECRET\"} }" \
#   "https://appengine.googleapis.com/v1/apps/$PROJECT_ID?alt=json&update_mask=iap"

  gcloud iap web enable --resource-type=app-engine --oauth2-client-id=$IAP_CLIENT_ID --oauth2-client-secret=$IAP_CLIENT_SECRET

  # Grant the current user access to the IAP
  echo -e "${COLOR}Granting user $USER_EMAIL access to the app through IAP...${NC}"
  gcloud iap web add-iam-policy-binding --resource-type=app-engine --member="user:$USER_EMAIL" --role='roles/iap.httpsResourceAccessor'
}


deploy_all() {
  check_billing
  enable_apis
  create_gae
  set_iam_permissions
  update_git_commit
  deploy_files
  deploy_app
  # NOTE: crerate_iap will fail if the current project isn't in a Cloud Org
  create_iap
}


_list_functions() {
  # list all functions in this file not starting with "_"
  declare -F | awk '{print $3}' | grep -v "^_"
}


if [[ $# -eq 0 ]]; then
  echo "USAGE: $0 target1 target2 ..."
  echo "  where supported targets:"
  _list_functions
else
  for i in "$@"; do
    if declare -F "$i" > /dev/null; then
      "$i"
      exitcode=$?
      if [ $exitcode -ne 0 ]; then
        echo "Breaking script as command '$i' failed"
        exit $exitcode
      fi
    else
      echo -e "${RED}Function '$i' does not exist.${NC}"
    fi
  done
fi

popd > /dev/null
