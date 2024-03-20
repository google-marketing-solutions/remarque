# Remarque

_This is not an officially supported Google product._

## Problem

The tool leverages Customer Match user lists in Google Ads using user ids from
GA4 data (via streaming to BigQuery).

The tool covers two big use-cases:

1. Unclear benefits of Remarketing Campaigns (ACe, App Campaigns for Engagement)
   in Google Ads for customers.
2. A need for a tool for audience management based on user ids and customer match

## Solution

The solution provides means for clients to create user segments at any stage of
their funnel, split them into treatment and control groups, conduct
experiments (i.e run campaigns with customer match audience of test users)
and analyse results by comparing conversions from treatment and control groups.

It can used for conducting A/B to verify how well ACe campaigns work for you. Or
(alternatevely or after that) to regularly upload your crafted audiences for
remarketing with controlling of results.

## Prerequisites

1. Firebase streaming to BigQuery. There should be at least one of the following events included: session_start, first_open, plus events that will be used for sampling users into audiences
2. Google Ads API access (developer token)

## Installation

1. Install application
   Installation is easier to execute in Cloud Shell but actually you can run it anywhere.
   Open Cloud Shell and execute in the terminal:

```
git clone https://github.com/google-marketing-solutions/remarque
cd remarque
./setup.sh deploy_all
```
Please make sure that the user under which you run `setup.sh` is has Owner role.

Run `setup.sh` without parameter to see the application service account
(PROJECT_ID@appspot.gserviceaccount.com)

Execution of `setup.sh` might fail if your Cloud project isn't in a Cloud Organization.
In that case you will need to manually create OAuth consent screen and
enable IAP for your AppEngine.

To redeploy app run:

```
./setup.sh deploy_app
```

2. Grant the service account access permissions to GA4 dataset.

3. Prepare Google Ads credentials: developer token, client_id, client_secret, refresh_token.
   See here how to authenticate and get a refresh token - https://github.com/google/ads-api-report-fetcher/blob/main/docs/how-to-authenticate-ads-api.md
   Please note, that the user under which a refresh token is generated should have write access to Google Ads MCC account specified in the application configuration.

4. Access your application
   After the application is deployed and accessible you won't have access to it because the OAuth consent screen should be switched to Production mode.
   To do this go to https://console.cloud.google.com/apis/credentials/consent
   If you don't get access after that, go to IAP settings (in Cloud Console) - https://console.cloud.google.com/security/iap and check IAP status is OK.
   If it's not, switch it off and on again. It should be OK after that. Also add all needed users here (as individual accounts or domains or Google groups).

All other settings should be done inside the application.

FYI: the configuration is kept on GCS gs://<project_id>/remarque/config.json

## Design

Roughly the application supports two use-cases:

1. Defining audiences
2. Executing audiences

_Defining audiences_ consists of creating one or more audiences in the application with some conditions for users to be captured into the audience. By 'users' here we mean advertising_id from GA4 streaming data in BigQuery.
There're two approaches here: using standard conditions ("include events", "exclude events", countries, app_id, etc) or using custom query. Custom query is meant to return users from GA4 picked by some custom conditions that can't be expressed via the standard approach.

_Executing audiences_ is an ETL-like workflow that consists of multiple steps described below.

### Sampling

We take GA4 tables in BigQuery (events\_\*) for a particular Firebase account.
From GA4 raw event data we create a table with users who are captured by an audience's conditions.
For example:

- Installed the application in last X days
- And have not converted into paying users (e.g. haven't registered and/or deposit), while have not removed the app.

Inputs and settings:

- App id (GA4 table can have events from several apps)
- Date period - by default, last 7 days
- Country or countries
- Events to analyze:
  - A list of events the user passed - by default ' first_open' but can be others as well
  - A list of events that didn't happen for the user - it's always a custom one (e.g. a registration event, like 'af_complete_registration')

A typical example - we choose users who:

- Installed the application ('first_open' event) in last 7 days
- And have not removed ('app_remove' event) it nor registered ('af_complete_registration' event) in it

Additionally we filter users (effectively advertising_id) by countries and app_id.

As a result of this stage we create a BQ table containing users (advertising_id) with above described conditionals that we'll split onto treatment/control groups on the next stage.

The table with sampled users has a name like this: `audience_{name}_all_YYYYMMDD`, where {name} is the name of an audience.

Each audience corresponds to a customer match user list in Google Ads.
We assume that there will be a campaign (or adgroup) targeted at each user list.

NOTE: It's important to note that for getting all users attributes we use a special table recreated every day in the Remarque dataset with all user data for last year - `users_normalized`. For getting user attributes we use two specific events: `first_open` and `session_start`, assuming that at least one of them (for each user) has a country (`geo.country`). As GA4 streaming has a limit of 1M events per day it's important to include those events into export (it can be one of them or both but if it's the only then it should have a country value).

### Splitting

For each audience defined on the previous step we split users extracted from GA4 into two groups (A and B). But not randomly, instead, using stratification. So splitting users in a way that ensures that each group has similar characteristics (i.e., they are "stratified" based on these characteristics).
The purpose of stratification is to reduce the variability between the groups, so that any observed differences in behavior or outcomes between the groups can be attributed to the treatment being tested, rather than to differences in the groups themselves.
This is the algorithm used for stratification - https://vict0rs.ch/2018/05/24/sample-multilabel-dataset/

Inputs and settings:

- treatment/control ratio - by default we split with 0.5 ratio but it can be customized

Stratification is conducted by the following features:

- Days since install
- Total number of sessions
- Device brand
- OS version
- Install traffic source

As a result we have pairs of two sets - 'treatment' and 'control'. Each pair is per audience. A pair correlates to an ad campaign (or an adgroup) that should be created in Google Ads and targets a 'treatment' group.

Each group is saved as a table with corresponding suffix (`_test` and `_control`).

For example, if we have an audience 'test' then the following tables will be created every day:

- audience_test_all_YYYYMMDD - sampled users for a day YYYYMMDD ("user segment")
- audience_test_test_YYYYMMDD - treatment users (list of user_id to be uplaoded to Google Ads)
- audience_test_control_YYYYMMDD - control users

At this stage we also take into account an audience's TTL setting - _time to live_, it's a number of days to keep users in treatment groups. So before uploading test groups we take users from yesterday that have TTL>1 (if they were not captured by today's sampling) and add them into today's treatment group.

### Upload Audiences

Upload treatment users from each group to Google Ads as Customer Match user list - see [Customer Match | Google Ads API](https://developers.google.com/google-ads/api/docs/remarketing/audience-types/customer-match#customer_match_with_mobile_device_ids).
For uploading users to a user list we use 'rewrite' mode (remove all existing users in the user list).

### Run campaigns (ACe)

At the moment we don't automate this. It's up to you to start campaigns in Google Ads using uploaded customer match user lists.
Campaigns should be running at least till X% of treatment users are covered.

### Analyze results

For any point in time the application can present conversion results for treatment and control users (per audience).
For example, if we created an audience for users who registered in the app but didn't deposit then for analysis we'll provide two graphs: cumulative conversions (the number of 'deposit' events) for each day for two groups (treatment and control).
The expected picture - a graph for treatment users is growing faster.

Besides conversion increment graphs the app provide the following statistical metrics to help assess the results:

- P-value

### Incrementality

In this section we'll look at the process in dynamic.
Once installed the tool is supposed to be running every day. For this we set up a schedule in each configuration. It creates a Scheduler Job in GCP that calls the application's endpoint. Effectively it does sampling, splitting and uploading of audiences to Ads for each audience. But it's important to note the process on a day can't work independently of previous days' results. If on another day we just split sampled users to treatment/control groups (using stratification) it can be easily turned out that a user that was yesterday in the control group came to the treatment group (and vice versa). So splitting is dependent on previous days to guarantee 'user affinity' which can be described as: once a user got into a treatment group it should stay (if other conditions are satisfied) in it and vice versa once a user got into a control group it can be only there.

All this give us a more detailed algorithm:
Once users from GA4 were fetched by an audience's criteria we remove all users that we saw in previous days and split into A/B groups only the new users. After we got new treatment and control groups we re-add users excluded on the previous step (treatment users from previous days into today's treatment group and same for control group).

Another feature that affects user distribution and user lists uploaded to Ads is user ttl - "time to live". By default if a user is fetched by an audience criteria on a day N they can easily fall out of the audience on the next day. So such a user will be exposed to ACe only for one day. To prevent this an audience can adjust its TTL to set it bigger than 1 (the default). In that case users once captured into a test group will stay in test groups for N days at least (where N is the TTL).
