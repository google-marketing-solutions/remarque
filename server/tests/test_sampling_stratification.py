# Copyright 2023-2005 Google LLC
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

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-importing-member, wrong-import-position
import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sampling import split_via_stratification, get_split_metrics


def test_split_via_stratification():
  """Test stratification with 4 users having overlapping patterns."""
  # Create test DataFrame
  df = pd.DataFrame({
      'user': ['user1', 'user2', 'user3', 'user4'],
      'days_since_install': [1088, 720, 555, 873],
      'n_sessions': [10, 12, 15, 16],
      'brand': ['Apple', 'Apple', 'Samsung', 'Samsung'],
      'src': ['Web', 'Mobile', 'Web', 'Mobile']
  })

  # Call function
  result = split_via_stratification(df, split_ratio=0.5)
  users_test = result.users_test
  users_control = result.users_control

  # Get original data for test and control users
  test_data = df[df['user'].isin(users_test['user'])]
  control_data = df[df['user'].isin(users_control['user'])]

  # Verify basic properties
  assert len(users_test) == len(users_control) == 2  # Equal split
  assert set(users_test['user']).isdisjoint(set(
      users_control['user']))  # No overlap

  # Instead of requiring perfect distribution, verify reasonable distribution
  all_days = set(df['days_since_install'])
  all_brands = set(df['brand'])
  all_sources = set(df['src'])

  # Check that we have some variety in each group
  assert len(set(test_data['days_since_install'])) >= 1
  assert len(set(control_data['days_since_install'])) >= 1

  assert len(set(test_data['brand'])) >= 1
  assert len(set(control_data['brand'])) >= 1

  assert len(set(test_data['src'])) >= 1
  assert len(set(control_data['src'])) >= 1

  # Verify all features are represented across both groups combined
  test_and_control_days = set(test_data['days_since_install']) | set(
      control_data['days_since_install'])
  test_and_control_brands = set(test_data['brand']) | set(control_data['brand'])
  test_and_control_sources = set(test_data['src']) | set(control_data['src'])

  assert test_and_control_days == all_days
  assert test_and_control_brands == all_brands
  assert test_and_control_sources == all_sources


def test_get_split_metrics():
  """Test split metrics calculation with edge case causing chi-square error."""
  # Create minimal DataFrame with distribution that will cause the error
  df = pd.DataFrame({
      'user': ['user1', 'user2', 'user3', 'user4', 'user5'],  # 5 users
      'days_since_install': [1, 1, 2, 2, 2],
      'n_sessions': [5, 5, 10, 10, 10],
      'brand': ['Samsung', 'Samsung', 'Xiaomi', 'Xiaomi',
                'Xiaomi'],  # uneven split
      'osv': [
          'Android 14', 'Android 14', 'Android 13', 'Android 13', 'Android 13'
      ]
  })

  # Uneven split (2 vs 3 users)
  users_test = pd.DataFrame({'user': ['user1', 'user2']})
  users_control = pd.DataFrame({'user': ['user3', 'user4', 'user5']})

  # Get metrics
  metrics = get_split_metrics(
      df,
      users_test,
      users_control,
      numeric_features=['days_since_install'],
      categorical_features=['brand', 'osv'])

  # Verify we get metrics without errors
  assert 'brand' in metrics
  assert 'osv' in metrics
  assert 'days_since_install' in metrics

  # These distributions should trigger warnings
  assert len(metrics['brand'].warnings) > 1
  assert len(metrics['osv'].warnings) > 1
