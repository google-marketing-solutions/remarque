# Remarque

## Problem
We often see low adoption of Remarketing Campaigns (ACe) among Google Ads
customers

## Solution
The solution provides means for clients to create user segments at any stage of
their funnel, equally split them into test and control groups, conduct
experiments (that is run campaigns with customer match audience of test users)
and analyse any possible gain (hopefully) by comparing conversions from test and
control groups.

## Installation
Generate an auth cookie on:
https://professional-services.googlesource.com/new-password

Open Cloud Shell and execute in the terminal:

```
git clone https://professional-services.googlesource.com/solutions/remarque
cd remarque
./setup.sh deploy_all
```

Run setup.sh without parameter to see the application service account
(PROJECT_ID@appspot.gserviceaccount.com)

Execution of `setup.sh` might fail if the project isn't in a Cloud Organization. In that case you will need to manually create OAuth consent screen and enable IAP for your AppEngine.

To redeploy app run:
```
./setup.sh deploy_app
```

Grant the service account access permissions to GA4 dataset.

Prepare Google Ads credentials: developer token, client_id, client_secret, refresh_token.
See here how to authenticate and get a refresh token - https://github.com/google/ads-api-report-fetcher/blob/main/docs/how-to-authenticate-ads-api.md

Please note, that the user under which a refresh token is generated should have write access to Google Ads MCC account specified in the application configuration.

After the app is deployed and accessible you won't have access to it because the OAuth consent screen should be switched to Production mode. 
For this go to https://console.cloud.google.com/apis/credentials/consent

If you don't get access after that go to IAP settings (in Cloud Console) - https://console.cloud.google.com/security/iap and check IAP status is OK. 
If it's not, switch it off and on again. It should be OK after that. Also add all needed users here (as individual accounts or domains or Google groups).

All other settings are made inside the application.

FYI: the configuration is kept on GCS gs://<project_id>/remarque/config.json
