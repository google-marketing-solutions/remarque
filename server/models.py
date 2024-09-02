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
from typing import Literal
from datetime import date
from dataclasses import dataclass


class Audience:
  name = ''
  id = ''
  app_id = ''
  table_name = ''
  countries: list[str]
  events_include: list[str]
  events_exclude: list[str]
  days_ago_start = 0
  days_ago_end = 0
  user_list = ''
  created = None
  mode: Literal['off'] | Literal['test'] | Literal['prod'] = 'off'
  ttl: 1
  split_ratio: float = None
  query = ''

  def __init__(self) -> None:
    self.countries = []
    self.events_include = []
    self.events_exclude = []

  def ensure_table_name(self):
    if not self.table_name:
      self.table_name = 'audience_' + self.name

  def to_dict(self) -> dict:
    res = {
        'name': self.name,
        'id': self.id,
        'app_id': self.app_id,
        'table_name': self.table_name,
        'countries': self.countries,
        'events_include': self.events_include,
        'events_exclude': self.events_exclude,
        'days_ago_start': self.days_ago_start,
        'days_ago_end': self.days_ago_end,
        'user_list': self.user_list,
        'created': self.created,
        'mode': self.mode,
        'query': self.query,
        'ttl': self.ttl,
        'split_ratio': self.split_ratio,
    }
    return res

  def __str__(self):
    return str(self.to_dict())

  def __repr__(self):
    return 'Audience: ' + str(self.to_dict())

  @staticmethod
  def from_dict(map: dict):
    self = Audience()
    self.name = map.get('name', None)
    self.id = map.get('id', None)
    self.app_id = map.get('app_id', None)
    self.table_name = map.get('table_name', None)
    self.countries = map.get('countries', None)
    self.events_include = map.get('events_include', None)
    self.events_exclude = map.get('events_exclude', None)
    self.days_ago_start = map.get('days_ago_start', None)
    self.days_ago_end = map.get('days_ago_end', None)
    self.user_list = map.get('user_list', None)
    self.created = map.get('created', None)
    self.mode = map.get('mode', 'off')
    self.query = map.get('query', None)
    self.ttl = map.get('ttl', None)
    self.split_ratio = map.get('split_ratio', None)
    return self


@dataclass
class AudienceLog:
  name: str
  date: date
  job_resource_name: str
  uploaded_user_count: int
  new_test_user_count: int
  new_control_user_count: int
  test_user_count: int
  control_user_count: int
  total_test_user_count: int
  total_control_user_count: int
  failed_user_count: int

  def to_dict(self):
    return {
        'date': self.date,
        'job_resource_name': self.job_resource_name,
        'uploaded_user_count': self.uploaded_user_count,
        'new_test_user_count': self.new_test_user_count,
        'new_control_user_count': self.new_control_user_count,
        'failed_user_count': self.failed_user_count,
        'test_user_count': self.test_user_count,
        'control_user_count': self.control_user_count,
        'total_test_user_count': self.total_test_user_count,
        'total_control_user_count': self.total_control_user_count,
    }
