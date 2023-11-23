from typing import Literal
from datetime import date, datetime, timedelta, timezone
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
  mode: Literal['off']|Literal['test']|Literal['prod'] = 'off'
  ttl: 1
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
      "name": self.name,
      "id": self.id,
      "app_id": self.app_id,
      "table_name": self.table_name,
      "countries": self.countries,
      "events_include": self.events_include,
      "events_exclude": self.events_exclude,
      "days_ago_start": self.days_ago_start,
      "days_ago_end": self.days_ago_end,
      "user_list": self.user_list,
      "created": self.created,
      "mode": self.mode,
      "query": self.query,
      "ttl": self.ttl,
    }
    return res

  def __str__(self):
    return str(self.to_dict())

  def __repr__(self):
        return "Audience: " + str(self.to_dict())


  @staticmethod
  def from_dict(dict: dict):
    self = Audience()
    self.name = dict.get("name", None)
    self.id = dict.get("id", None)
    self.app_id = dict.get("app_id", None)
    self.table_name = dict.get("table_name", None)
    self.countries = dict.get("countries", None)
    self.events_include = dict.get("events_include", None)
    self.events_exclude = dict.get("events_exclude", None)
    self.days_ago_start = dict.get("days_ago_start", None)
    self.days_ago_end = dict.get("days_ago_end", None)
    self.user_list = dict.get("user_list", None)
    self.created = dict.get("created", None)
    self.mode = dict.get("mode", 'off')
    self.query = dict.get("query", None)
    self.ttl = dict.get("ttl", None)
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
      "date": self.date,
      "job_resource_name": self.job_resource_name,
      "uploaded_user_count": self.uploaded_user_count,
      "new_test_user_count": self.new_test_user_count,
      "new_control_user_count": self.new_control_user_count,
      "failed_user_count": self.failed_user_count,
      "test_user_count": self.test_user_count,
      "control_user_count": self.control_user_count,
      "total_test_user_count": self.total_test_user_count,
      "total_control_user_count": self.total_control_user_count,
    }