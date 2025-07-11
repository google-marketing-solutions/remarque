# Copyright 2023-2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Domain model classes."""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-importing-member, wrong-import-position
from dataclasses import dataclass
from datetime import date
from typing import Literal, Any
import pandas as pd
import math
import numpy as np


class Audience:
  """An audience definition."""
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

  @classmethod
  def from_dict(cls, obj: dict):
    self = cls()
    self.name = obj.get('name', None)
    self.id = obj.get('id', None)
    self.app_id = obj.get('app_id', None)
    self.table_name = obj.get('table_name', None)
    self.countries = obj.get('countries', None)
    self.events_include = obj.get('events_include', None)
    self.events_exclude = obj.get('events_exclude', None)
    self.days_ago_start = obj.get('days_ago_start', None)
    self.days_ago_end = obj.get('days_ago_end', None)
    self.user_list = obj.get('user_list', None)
    self.created = obj.get('created', None)
    self.mode = obj.get('mode', 'off')
    self.query = obj.get('query', None)
    self.ttl = obj.get('ttl', None)
    self.split_ratio = obj.get('split_ratio', None)
    return self


@dataclass
class AudienceLog:
  """An audience's log entry."""
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


def to_float(v: Any) -> float | None:
  return float(v) if v is not None and (math.isfinite(v) or
                                        np.isfinite(v)) else None


@dataclass
class FeatureMetrics:
  """Stat metrics of a split for one feature."""
  mean_ratio: float | None = None
  std_ratio: float | None = None
  ks_statistic: float | None = None
  p_value: float | None = None
  js_divergence: float | None = None
  proportion_diffs: dict[str, dict[str, float]] | None = None
  max_diff: float | None = None
  warnings: dict[str, str] | None = None

  def __post_init__(self):
    self.mean_ratio = to_float(self.mean_ratio)
    self.std_ratio = to_float(self.std_ratio)
    self.ks_statistic = to_float(self.ks_statistic)
    self.p_value = to_float(self.p_value)
    self.js_divergence = to_float(self.js_divergence)
    self.max_diff = to_float(self.max_diff)


@dataclass
class DistributionData:
  """A feature distribution after split in test/control groups."""
  feature_name: str
  is_numeric: bool
  categories: list[str] | None = None
  test_distribution: list[float] | None = None
  control_distribution: list[float] | None = None


@dataclass
class SplittingResult:
  """Result of splitting."""
  users_test: pd.DataFrame
  users_control: pd.DataFrame
  metrics: dict[str, FeatureMetrics]
  distributions: list[DistributionData]
