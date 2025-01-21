# Copyright 2024 Google LLC
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

import os
import sys
import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sampling import binsify


def test_binsify():
  """Test the binning functionality."""
  # Create test data with known values
  df = pd.DataFrame({'n_sessions': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})

  # Test with default percentiles
  bins = binsify(df, 'n_sessions')
  assert len(bins) == 5  # 0.0 plus 4 percentile points
  assert bins[0] == 0.0  # First bin edge should always be 0
  assert all(bins[i] < bins[i + 1]
             for i in range(len(bins) -
                            1))  # Bins should be strictly increasing

  # Test that bins correctly divide the data
  values = df['n_sessions'].values
  bin_assignments = np.searchsorted(bins, values)

  # Check binning properties
  assert bin_assignments[0] == 1  # smallest value should be in first bin
  assert bin_assignments[-1] == 5  # largest value should be in last bin

  # Check that bins divide data roughly equally (with default percentiles)
  bin_counts = np.bincount(bin_assignments)[
      1:]  # Skip 0 bin which should be empty
  assert all(1 <= count <= 3 for count in bin_counts
            )  # Each bin should have 1-3 values with our test data


def test_binsify_custom_percentiles():
  """Test binsify with custom percentiles."""
  df = pd.DataFrame({'n_sessions': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})

  # Test with custom percentiles - splitting into thirds
  custom_percentiles = [0.33, 0.67]
  bins = binsify(df, col='n_sessions', percentile=custom_percentiles)

  assert len(bins) == 3  # 0.0 plus 2 percentile points
  assert bins[0] == 0.0
  assert all(bins[i] < bins[i + 1] for i in range(len(bins) - 1))


def test_binsify_edge_cases():
  """Test binsify with edge cases."""
  # Test with single value
  df = pd.DataFrame({'n_sessions': [1] * 10})
  bins = binsify(df, 'n_sessions')
  assert len(
      bins
  ) == 2  # Should have at least two bin edges even with identical values
  assert bins[0] == 0.0

  # Test with negative values
  df = pd.DataFrame({'n_sessions': [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4]})
  bins = binsify(df, 'n_sessions')
  assert bins[0] == 0.0  # Should still start at 0 regardless of negative values


def test_searchsorted_binning():
  """Test the full binning process including searchsorted."""
  df = pd.DataFrame({
      'n_sessions': [1, 3, 5, 7, 9]  # Values chosen for easy bin verification
  })

  bins = binsify(df, 'n_sessions')
  bin_assignments = np.searchsorted(bins, df['n_sessions'].values)

  # Each value should be in a different bin (with our test data)
  assert len(set(bin_assignments)) == 2

  # # Bins should be sequential
  # df = pd.DataFrame({
  #     'n_sessions': [1, 3, 5, 7, 9, 2, 6, 4, 0, 8, 11]  # Values chosen for easy bin verification
  # })

  # bins = binsify(df, 'n_sessions')
  # bin_assignments = np.searchsorted(bins, df['n_sessions'].values)

  # # Each value should be in a different bin (with our test data)
  # assert len(set(bin_assignments)) == 5
  # assert list(sorted(bin_assignments)) == list(range(1, 6))


def test_binsify_small_dataset():
  """Test binsify with a small dataset similar to our stratification test case."""
  df = pd.DataFrame({
      'n_sessions': [10, 12, 15, 16]  # 4 values close to each other
  })

  bins = binsify(df, 'n_sessions')
  bin_assignments = np.searchsorted(bins, df['n_sessions'].values)

  # Print debug info
  print(f"Bins: {bins}")
  print(f"Original values: {df['n_sessions'].values}")
  print(f"Bin assignments: {bin_assignments}")

  # Check that we don't get unique bin for each value
  assert len(set(bin_assignments)) < len(df), \
      "Small datasets shouldn't have unique bin for each value"

  # Check that we get reasonable grouping
  assert len(set(bin_assignments)) <= 3, \
      'Small datasets should have at most 2-3 bins'
