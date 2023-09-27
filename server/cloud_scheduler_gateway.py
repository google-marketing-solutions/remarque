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

from typing import Optional
from google.cloud import scheduler_v1
from google.api_core import exceptions
from google.auth import credentials
from config import Config, ConfigTarget
import os
from dataclasses import dataclass

@dataclass
class Job:
  enabled: bool
  schedule: str
  schedule_timezone: str
  name: Optional[str] = None

  def __init__(self, enabled: bool, *, schedule: str = None, schedule_timezone: str = None, schedule_time: str = None):
    self.name = None
    self.enabled: bool = enabled
    self.schedule: str = schedule
    self.schedule_timezone: str = schedule_timezone
    self.schedule_time = schedule_time
    if schedule_time:
      parts = schedule_time.split(':')
      # create daily cron schedule for the time
      hours = parts[0]
      if hours.startswith("0"):
        hours = hours[1:]
      minutes = parts[1]
      if minutes == "00":
        minutes = "0"
      self.schedule = f"{minutes} {hours} * * *"
    elif schedule:
      # extract time from a croc schedule
      parts = schedule.split(' ')
      hours = parts[1]
      if hours.isdigit():
        if len(hours) == 1:
          hours = "0" + hours
      else:
        hours = "00"
      minutes = parts[0]
      if minutes.isdigit():
        if len(minutes) == 1:
          minutes = "0" + minutes
      else:
        minutes = "00"
      self.schedule_time = f"{hours}:{minutes}"


class CloudSchedulerGateway:

  def __init__(self, config: Config,
               credentials: credentials.Credentials):
    self.config = config
    self.credentials = credentials
    self.client = scheduler_v1.CloudSchedulerClient(credentials=credentials)
    # Then currently (at 2021 April) there're just two locations for Scheduler: us-west1 and europe-west1.

  def _get_job_id(self, target: str):
    return f'remarque_{target or "default"}'


  def get_job(self, target_name: str) -> Job:
    project_id = self.config.project_id
    location_id = self.config.scheduler_location_id
    job_id = self._get_job_id(target_name)
    job_name = f'projects/{project_id}/locations/{location_id}/jobs/{job_id}'
    try:
      job = self.client.get_job(scheduler_v1.GetJobRequest(name=job_name))
    except exceptions.NotFound:
      return Job(enabled=False)
    res = Job(job.state == scheduler_v1.Job.State.ENABLED, schedule=job.schedule, schedule_timezone=job.time_zone)
    res.name = job_name
    return res


  def update_job(self, target: ConfigTarget, job: Job):
    project_id = self.config.project_id
    location_id = self.config.scheduler_location_id
    job_id = self._get_job_id(target.name)
    job_name = f'projects/{project_id}/locations/{location_id}/jobs/{job_id}'
    try:
      cloud_job = self.client.get_job(scheduler_v1.GetJobRequest(name=job_name))
      if job.enabled and (
        cloud_job.state != scheduler_v1.Job.State.ENABLED or
        cloud_job.schedule != job.schedule or
        cloud_job.time_zone != job.schedule_timezone):
        # to enable
        gae_service = os.getenv('GAE_SERVICE')
        routing = scheduler_v1.AppEngineRouting()
        routing.service = gae_service
        job = scheduler_v1.Job(
          name=job_name,
          app_engine_http_target=scheduler_v1.AppEngineHttpTarget(
            app_engine_routing=routing,
            relative_uri="/api/process",
            http_method=scheduler_v1.HttpMethod.POST,
            body=b"{}",
          ),
          schedule = job.schedule,
          time_zone = job.schedule_timezone,
        )
        self.client.update_job(scheduler_v1.UpdateJobRequest(
          job = job,
          #update_mask = ['schedule','timeZone'],
        ))
      elif not job.enabled:
        # to disable
        self.delete_scheduler_job(project_id, location_id, job_id)
    except exceptions.NotFound:
      if job.enabled:
        # there's no Job in GCP but we'are asked to have one
        self.create_scheduler_job(project_id, location_id, target.name, job)
      # otherwise there's no job but it's not needed, that's ok


  # def get_scheduler_jobs(
  #       self, project_id: str, location_id: str,):
  #   client = scheduler_v1.CloudSchedulerClient()
  #   jobs = client.list_jobs(
  #     request=scheduler_v1.ListJobsRequest(
  #       client.common_location_path(project_id, location_id)
  #     ),
  #   )
  #   for job in jobs:
  #     print(job)
  #     print(job.schedule, job.schedule_time, job.state, job.status, job.name, job.time_zone)
  #   return jobs


  def create_scheduler_job(
      self, project_id: str, location_id: str, target_name: str, job: Job) -> scheduler_v1.Job:
    """Create a job with an App Engine target via the Cloud Scheduler API.

    Args:
      project_id: The Google Cloud project id.
      location_id: The location for the job.
      service_id: An unique service id for the job.

    Returns:
      The created job.
    """
    job_id = self._get_job_id(target_name)
    job_name = f'projects/{project_id}/locations/{location_id}/jobs/{job_id}'
    job = scheduler_v1.Job(
      name=job_name,
      app_engine_http_target=scheduler_v1.AppEngineHttpTarget(
        app_engine_routing=scheduler_v1.AppEngineRouting(),
        relative_uri="/api/process",
        http_method=scheduler_v1.HttpMethod.POST,
        body=b"{}",
      ),
      schedule = job.schedule,
      time_zone = job.schedule_timezone,
    )
    response = self.client.create_job(
      scheduler_v1.CreateJobRequest(
        parent=self.client.common_location_path(project_id, location_id),
        job=job,
      )
    )
    return response


  def delete_scheduler_job(
      self, project_id: str, location_id: str, job_id: str) -> None:
    """Delete a job via the Cloud Scheduler API.

    Args:
      project_id: The Google Cloud project id.
      location_id: The location for the job to delete.
      job_id: The id of the job to delete.
    """

    # Create a client.
    client = scheduler_v1.CloudSchedulerClient()

    # Use the client to send the job deletion request.
    client.delete_job(
      scheduler_v1.DeleteJobRequest(
        name=client.job_path(project_id, location_id, job_id)
      )
    )
