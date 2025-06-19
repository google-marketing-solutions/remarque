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
"""Utility functions."""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-importing-member, wrong-import-position
import datetime


def format_duration(duration: datetime.timedelta):
  total_seconds = duration.total_seconds()
  hours, remainder = divmod(total_seconds, 3600)
  minutes, seconds = divmod(remainder, 60)
  milliseconds = duration.microseconds // 1000
  if hours > 0:
    return '{:02}:{:02}:{:02}.{:03d}'.format(
        int(hours), int(minutes), int(seconds), milliseconds)
  return '{:02}:{:02}.{:03d}'.format(int(minutes), int(seconds), milliseconds)
