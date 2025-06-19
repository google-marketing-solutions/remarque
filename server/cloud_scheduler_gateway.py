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
"""Cloud Scheduler utility methods."""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-importing-member, wrong-import-position
from typing import Optional
from datetime import datetime, timedelta
from google.cloud import scheduler_v1
from google.cloud import logging_v2
from google.api_core import exceptions
from google.auth.credentials import Credentials
from config import Config, ConfigTarget
import os
from dataclasses import dataclass, field


@dataclass
class Job:
  """Scheduler Job description.

  Attributes:
    enabled: A boolean indicating if the job is enabled.
    schedule: A string with croc-formatted schedule.
    schedule_timezone: A timezone name.
    name: A job name.
    runs:
      An ordered list of past runs, where each run is tuple of date and status.
  """
  enabled: bool
  schedule: str
  schedule_timezone: str
  name: Optional[str] = None
  runs: list[tuple[str, str]] = field(default_factory=list)

  def __init__(self,
               enabled: bool,
               *,
               schedule: str = None,
               schedule_timezone: str = None,
               schedule_time: str = None):
    self.name = None
    self.runs = []
    self.enabled: bool = enabled
    self.schedule: str = schedule
    self.schedule_timezone: str = schedule_timezone
    self.schedule_time = schedule_time
    if schedule_time:
      parts = schedule_time.split(':')
      # create daily cron schedule for the time
      hours = parts[0]
      if hours.startswith('0'):
        hours = hours[1:]
      minutes = parts[1]
      if minutes == '00':
        minutes = '0'
      self.schedule = f'{minutes} {hours} * * *'
    elif schedule:
      # extract time from a croc schedule
      parts = schedule.split(' ')
      hours = parts[1]
      if hours.isdigit():
        if len(hours) == 1:
          hours = '0' + hours
      else:
        hours = '00'
      minutes = parts[0]
      if minutes.isdigit():
        if len(minutes) == 1:
          minutes = '0' + minutes
      else:
        minutes = '00'
      self.schedule_time = f'{hours}:{minutes}'


class CloudSchedulerGateway:
  """Gateway to work with Scheduler Job.

  Attributes:
    config: The application config.
    credentials: A credentials object.
    client: An instance of `google.cloud.scheduler_v1.CloudSchedulerClient`.
  """

  def __init__(self, config: Config, credentials: Credentials):
    self.config = config
    self.credentials = credentials
    self.client = scheduler_v1.CloudSchedulerClient(credentials=credentials)

  def _get_job_id(self, target: str):
    return f'remarque_{target or "default"}'

  def get_job(self,
              target_name: str,
              load_logs: bool,
              load_last_days: int = 10) -> Job:
    """Get a target's job, optionally with run history.

    Args:
      target_name: A target name.
      load_logs: True to load run history.
      load_last_days: A number of days to load history for (default 10).
    """
    project_id = self.config.project_id
    location_id = self.config.scheduler_location_id
    job_id = self._get_job_id(target_name)
    job_name = f'projects/{project_id}/locations/{location_id}/jobs/{job_id}'
    try:
      job = self.client.get_job(scheduler_v1.GetJobRequest(name=job_name))
    except exceptions.NotFound:
      return Job(enabled=False)
    res = Job(
        job.state == scheduler_v1.Job.State.ENABLED,
        schedule=job.schedule,
        schedule_timezone=job.time_zone)
    res.name = job_name
    if load_logs:
      logging_client = logging_v2.Client()
      start_time = datetime.now() - timedelta(days=load_last_days)
      log_filter = (
          'resource.type="cloud_scheduler_job" '
          f'AND resource.labels.job_id="{job_id}" '
          f'AND timestamp >= "{start_time.isoformat(timespec="milliseconds")}Z"'
      )
      entries = logging_client.list_entries(filter_=log_filter)
      for entry in entries:
        timestamp = entry.timestamp.isoformat()
        if entry.payload.get('@type') != \
          'type.googleapis.com/google.cloud.scheduler.logging.AttemptFinished':
          continue
        status = 'Success' if entry.http_request.get(
            'status') == 200 else 'Failure'
        res.runs.append((timestamp, status))

    return res

  def update_job(self, target: ConfigTarget, job: Job) -> None:
    """Update a target's job.

    Args:
      target: A target.
      job: The target's Cloud job to update.
    """
    project_id = self.config.project_id
    location_id = self.config.scheduler_location_id
    job_id = self._get_job_id(target.name)
    job_name = f'projects/{project_id}/locations/{location_id}/jobs/{job_id}'
    cloud_job = None
    try:
      cloud_job = self.client.get_job(scheduler_v1.GetJobRequest(name=job_name))
    except exceptions.NotFound:
      if not job.enabled:
        # no job exists and we're asked to remove, done
        return

    if cloud_job and not job.enabled:
      # to disable we delete the job
      self._delete_scheduler_job(project_id, location_id, job_id)
    elif job.enabled:
      gae_service = os.getenv('GAE_SERVICE')
      routing = scheduler_v1.AppEngineRouting()
      routing.service = gae_service
      uri = '/api/process?target=' + (
          target.name if target.name and target.name != 'default' else '')
      print(f'Setting up scheduler for AppEngine at {uri}')
      job = scheduler_v1.Job(
          name=job_name,
          app_engine_http_target=scheduler_v1.AppEngineHttpTarget(
              app_engine_routing=routing,
              relative_uri=uri,
              http_method=scheduler_v1.HttpMethod.POST,
              body=b'{}',
          ),
          schedule=job.schedule,
          time_zone=job.schedule_timezone,
      )
      if cloud_job:
        self.client.update_job(scheduler_v1.UpdateJobRequest(job=job))
      else:
        self.client.create_job(
            scheduler_v1.CreateJobRequest(
                parent=self.client.common_location_path(project_id,
                                                        location_id),
                job=job,
            ))

  def delete_job(self, target_name: str) -> None:
    """Delete a target's job.

    Args:
      target_name: A target name.
    """
    project_id = self.config.project_id
    location_id = self.config.scheduler_location_id
    job_id = self._get_job_id(target_name)
    self._delete_scheduler_job(project_id, location_id, job_id)

  def _delete_scheduler_job(self, project_id: str, location_id: str,
                            job_id: str) -> None:
    """Delete a job via the Cloud Scheduler API.

    Args:
      project_id: The Google Cloud project id.
      location_id: The location for the job to delete.
      job_id: The id of the job to delete.
    """
    client = scheduler_v1.CloudSchedulerClient()

    try:
      client.delete_job(
          scheduler_v1.DeleteJobRequest(
              name=client.job_path(project_id, location_id, job_id)))
    except exceptions.NotFound:
      # no job exists and we're asked to remove, done
      pass
