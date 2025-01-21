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
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sampling import make_encoding


@pytest.fixture
def sample_df():
  """Create a sample DataFrame for testing."""
  return pd.DataFrame({
      'user': ['u1', 'u2', 'u3', 'u4'],
      'brand': ['Apple', 'Samsung', 'Apple', 'Huawei'],
      'age': [25, 30, 35, 40],
      'osv': ['15', '12', '15', '11']
  })


def test_basic_encoding(sample_df):
  """Test basic encoding functionality."""
  # Define test parameters
  exclude_cols = ['user']
  all_cols = ['user', 'brand', 'age', 'osv']

  # Run encoding
  result_df, cat_cols = make_encoding(sample_df, exclude_cols, all_cols)

  # Verify categorical columns were detected correctly
  assert set(cat_cols) == {'brand', 'osv'}

  # Verify all columns are present
  assert set(result_df.columns) == set(all_cols)

  # Verify categorical columns were encoded as integers
  assert result_df['brand'].dtype == np.int64
  assert result_df['osv'].dtype == np.int64

  # Verify numerical columns remained unchanged
  assert result_df['age'].equals(sample_df['age'])

  # Verify excluded columns remained unchanged
  assert result_df['user'].equals(sample_df['user'])


def test_invalid_columns():
  """Test handling of invalid column names."""
  df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})

  with pytest.raises(
      ValueError, match='all_cols contains columns not in DataFrame'):
    make_encoding(df, [], ['nonexistent'])

  with pytest.raises(
      ValueError, match='exclude_cols contains columns not in DataFrame'):
    make_encoding(df, ['nonexistent'], ['a', 'b'])


def test_no_categorical_columns():
  """Test handling of DataFrame with no categorical columns."""
  # Create DataFrame with only numerical columns
  num_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0], 'c': [7, 8, 9]})

  result_df, cat_cols = make_encoding(num_df, [], ['a', 'b', 'c'])

  # Verify no categorical columns were detected
  assert len(cat_cols) == 0

  # Verify DataFrame remained unchanged
  assert result_df.equals(num_df)


def test_null_handling(sample_df):
  """Test handling of null values."""
  # Add some null values
  df_with_nulls = sample_df.copy()
  df_with_nulls.loc[0, 'brand'] = None
  df_with_nulls.loc[1, 'osv'] = None

  exclude_cols = ['user']
  all_cols = ['user', 'brand', 'age', 'osv']

  result_df, cat_cols = make_encoding(df_with_nulls, exclude_cols, all_cols)

  assert isinstance(result_df.loc[0, 'brand'], (np.int64, int))
  assert isinstance(result_df.loc[1, 'osv'], (np.int64, int))


def test_index_preservation(sample_df):
  """Test that DataFrame index is preserved."""
  # Set a non-default index
  sample_df.index = ['a', 'b', 'c', 'd']

  result_df, _ = make_encoding(sample_df, ['user'],
                               ['user', 'brand', 'age', 'osv'])

  # Verify index was preserved
  assert (result_df.index == sample_df.index).all()


def test_dtype_preservation(sample_df):
  """Test that non-categorical dtypes are preserved."""
  # Add some different dtypes
  df = sample_df.copy()
  df['float_col'] = [1.1, 2.2, 3.3, 4.4]
  df['int_col'] = [1, 2, 3, 4]
  df['bool_col'] = [True, False, True, False]

  result_df, _ = make_encoding(df, ['user'], df.columns.tolist())

  # Verify dtypes were preserved for non-categorical columns
  assert result_df['float_col'].dtype == np.float64
  assert result_df['int_col'].dtype == np.int64
  assert result_df['bool_col'].dtype == np.bool_


def test_large_categorical_values():
  """Test handling of categorical columns with many unique values."""
  # Create DataFrame with large number of unique categories
  large_df = pd.DataFrame({
      'id': range(1000),
      'category': [f'cat_{i}' for i in range(1000)]
  })

  result_df, cat_cols = make_encoding(large_df, ['id'], ['id', 'category'])

  # Verify encoding worked correctly
  assert len(result_df['category'].unique()) == 1000
  assert result_df['category'].dtype == np.int64


def test_nullable_integer_handling():
  """Test handling of nullable integer columns with NA values."""
  # Create DataFrame with a regular integer column first
  df = pd.DataFrame({
      'user': ['u1', 'u2', 'u3'],
      'days_since_install': [1, 2, 3],
      'category': ['A', 'B', 'C']
  })

  # Convert to nullable integer and introduce NA
  df['days_since_install'] = df['days_since_install'].astype('Int64')
  df.loc[1, 'days_since_install'] = pd.NA

  result_df, cat_cols = make_encoding(
      df, ['user'], ['user', 'days_since_install', 'category'])

  # This should now fail with the same error as in production
  assert 'days_since_install' in result_df.columns
  assert not result_df['days_since_install'].isna().any()
  assert result_df['category'].dtype == np.int64


if __name__ == '__main__':
  pytest.main([__file__])
