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
"""Method for splitting users."""
import os
import numpy as np
import pandas as pd
import warnings
import logging
from scipy import stats
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from logger import logger
from models import FeatureMetrics, DistributionData, SplittingResult

warnings.filterwarnings('ignore')

logger = logger.getChild('sampling')
diagnostics_logger = logger.getChild('diagnostics')
enable_diagnostics = os.environ.get('ENABLE_DIAGNOSTIC_LOGGING',
                                    '').lower() == 'true'
if not enable_diagnostics:
  # Only critical errors will be logged
  diagnostics_logger.setLevel(logging.CRITICAL)


def make_encoding(df: pd.DataFrame,
                  exclude_cols: list[str],
                  all_cols: list[str],
                  encoder=None) -> tuple[pd.DataFrame, list[str]]:
  """Encode all categorical columns in a DataFrame as integers.

  Args:
    df: data in DF to be split.
    exclude_cols: A list of columns to exclude from encoding (e.g 'user').
    all_cols: A list of all columns to return in the final result.
    encoder: An optional pre-fitted encoder
      (useful if you want to apply same encoding to test data).

  Returns:
    Tuple of (encoded DataFrame, list of categorical column names).
  """
  if not all(col in df.columns for col in all_cols):
    raise ValueError('all_cols contains columns not in DataFrame')
  if not all(col in df.columns for col in exclude_cols):
    raise ValueError('exclude_cols contains columns not in DataFrame')

  # get a list of categorical columns (with 'object' data type, e.g. 'brand')
  cat_ix = df.drop(
      exclude_cols,
      axis=1).select_dtypes(include=['object']).columns.values.tolist()

  # Log and handle nullable integer columns
  dtypes_dct = dict(df.dtypes)
  for col in df.columns:
    if str(df[col].dtype) == 'Int64':
      if logger.isEnabledFor(logging.DEBUG):
        na_count = df[col].isna().sum()
        total_count = len(df[col])
        msg = (
            f'Column {col} has nullable Int64 dtype with '
            f'{na_count}/{total_count} ({na_count/total_count:.1%}) NA values')
        logger.debug(msg)
      # Convert to float64 and fill NA values with a sentinel value
      df[col] = df[col].astype('float64').fillna(-999)

  if not cat_ix:
    # No categorical columns to encode
    return df[all_cols], []

  transformers = [
      ('cat',
       OrdinalEncoder(handle_unknown='use_encoded_value',
                      unknown_value=-99), cat_ix)
  ]
  # Apply the specified transformations (OrdinalEncoder to categorical columns)
  # ('passthrough' means keep other columns unchanged)
  col_transform = ColumnTransformer(
      transformers=transformers, remainder='passthrough')

  # Store original dtypes for later restoration
  dtypes_dct = dict(df.dtypes)

  if encoder is None:
    encoder = col_transform.fit(df)

  # Transform data
  try:
    transformed = encoder.transform(df)
  except Exception as e:
    raise RuntimeError(f'Error during transformation: {str(e)}') from e

  # Gets names of columns that weren't transformed (numerical columns)
  non_cat_cols = [col for col in all_cols if col not in cat_ix]
  # Creates DataFrame from transformed data
  result_df = pd.DataFrame(
      transformed, columns=cat_ix + non_cat_cols, index=df.index)
  # Restore original data types
  result_df = result_df.astype({col: dtypes_dct[col] for col in non_cat_cols})
  # Ensures categorical columns are integers
  result_df[cat_ix] = result_df[cat_ix].astype(int)
  # now result_df has all the columns that the original df had,
  # but with the categorical ones transformed according to the rules defined
  #  via OrdinalEncoder

  return result_df[all_cols], cat_ix


def stratify(data: list[list[str]], classes: list[str],
             ratio: float) -> list[list]:
  """Stratifying procedure.

  Split data into test and control groups maintaining feature distributions.
  Algorithm is from: https://vict0rs.ch/2018/05/24/sample-multilabel-dataset/

  Args:
    data: List of lists - each sublist contains feature values for one user.
    classes: List of all possible feature values.
    ratio: Float between 0 and 1, portion of data for test group.

  Returns:
    list of lists with indices of test and control users.
  """
  # Organize data per label:
  # for each label l, per_label_data[l] contains the list of samples
  # in data which have this label
  per_label_data = {c: set() for c in classes}
  for i, d in enumerate(data):
    for l in d:
      per_label_data[l].add(i)
  # If data is:
  # data = [
  #     ['1', '100', '200'],  # row 0: days_install=1, brand=100, src=200
  #     ['2', '100', '201'],  # row 1: days_install=2, brand=100, src=201
  # ]
  # classes = ['1', '2', '100', '200', '201']
  # per_label_data would be:
  # {
  #     '1': {0},        # value '1' appears in row 0
  #     '2': {1},        # value '2' appears in row 1
  #     '100': {0, 1},   # value '100' appears in rows 0 and 1
  #     '200': {0},      # value '200' appears in row 0
  #     '201': {1}       # value '201' appears in row 1
  # }

  # number of samples (users)
  size = len(data)
  logger.debug('Stratification target: %s/%s (%s)', int(size * ratio), size,
               ratio)

  # Calculate ratios
  ratios = [ratio, 1 - ratio]
  # calculate size of each group (test/control)
  subset_sizes = [r * size for r in ratios]
  # calculate how many users with each particular feature value
  # should go to each group for ideal balance
  per_label_subset_sizes = {
      c: [r * len(per_label_data[c]) for r in ratios] for c in classes
  }

  # Log initial label distributions
  if diagnostics_logger.isEnabledFor(logging.DEBUG):
    for label, indices in per_label_data.items():
      diagnostics_logger.debug('Label %s: %s occurrences, target %.1f for test',
                               label, len(indices),
                               len(indices) * ratio)

  # Sort labels by frequency (most frequent first)
  sorted_labels = sorted(
      [(l, len(data)) for l, data in per_label_data.items()],
      key=lambda x: x[1],
      reverse=True  # Most frequent first
  )
  if diagnostics_logger.isEnabledFor(logging.DEBUG):
    diagnostics_logger.debug('Processing labels order (by frequency):')
    for label, count in sorted_labels:
      diagnostics_logger.debug('  %s: %s occurrences', label, count)

  # For each subset we want, the set of sample-ids which should end up in it
  stratified_data_ids = [set(), set()]

  # For each sample in the data set
  while size > 0:
    # Take next unprocessed label with most remaining samples
    available_labels = [(l, len(per_label_data[l]))
                        for l, _ in sorted_labels
                        if per_label_data[l]]
    if not available_labels:
      break

    label, count = max(available_labels, key=lambda x: x[1])
    diagnostics_logger.debug('Processing label %s with %s remaining instances',
                             label, count)

    # Process all samples with this label
    while per_label_data[label]:
      # Take one sample
      current_id = per_label_data[label].pop()
      # Find which subset (test/control) needs this label most -
      # So it's a two-level decision:
      #   1. First try to balance specific feature value
      #   2. If tied, try to balance overall group sizes
      #   3. If still tied, random choice
      test_needs_label = per_label_subset_sizes[label][0]
      control_needs_label = per_label_subset_sizes[label][1]
      if test_needs_label != control_needs_label:
        subset = 0 if test_needs_label > control_needs_label else 1
        diagnostics_logger.debug(
            'User %s assigned to %s based on label needs '
            '(test: %.1f, control: %.1f)', current_id,
            'test' if subset == 0 else 'control', test_needs_label,
            control_needs_label)
      else:
        if subset_sizes[0] != subset_sizes[1]:
          subset = 0 if subset_sizes[0] > subset_sizes[1] else 1
          diagnostics_logger.debug(
              'User %s assigned to %s based on group sizes '
              '(test: %s, control: %s)', current_id,
              'test' if subset == 0 else 'control', subset_sizes[0],
              subset_sizes[1])
        else:
          subset = np.random.choice([0, 1])
          diagnostics_logger.debug('User %s assigned to %s randomly',
                                   current_id,
                                   'test' if subset == 0 else 'control')

      # Store the sample's id in the selected subset
      stratified_data_ids[subset].add(current_id)

      # Update counts
      size -= 1  # one less sample to assign
      subset_sizes[subset] -= 1  # chosen group needs one less sample

      # Update per-label counts for all labels this sample has
      for l in data[current_id]:
        per_label_subset_sizes[l][subset] -= 1
        # Remove sample from other label sets
        if current_id in per_label_data[l]:
          per_label_data[l].remove(current_id)

  # Sort indices for consistency
  stratified_data_ids = [sorted(strat) for strat in stratified_data_ids]

  # Return the stratified indexes (0 - test users, 1 - control users)
  return stratified_data_ids


def binsify(df: pd.DataFrame,
            col: str,
            percentile: list[float] | None = None) -> list[float]:
  """Convert continuous values into bins based on percentiles.

  Creates bin edges starting from 0.0 and adding edges at specified percentiles.
  For example, with default percentiles [0.2, 0.4, 0.6, 0.8], creates 5 bins:
  - bin 1: values between 0 and 20th percentile
  - bin 2: values between 20th and 40th percentile
  - bin 3: values between 40th and 60th percentile
  - bin 4: values between 60th and 80th percentile
  - bin 5: values above 80th percentile

  Args:
    df: DataFrame containing the column to bin.
    col: Name of the column to bin.
    percentile: List of percentile points (between 0 and 1) for bin edges.

  Returns:
    List of bin edges starting with 0.0.
  """
  if not percentile:
    if len(df) < 10:
      percentile = [0.5]  # For small datasets, only split at median
    else:
      percentile = [0.2, 0.4, 0.6, 0.8]
  bins = [0.0]
  p = sorted(list(set(np.quantile(df[col].values, percentile))))
  bins.extend(p)
  return bins


def offset_features(df: pd.DataFrame, cat_features: list[str],
                    numeric_features: list[str]):
  """Offset categorical features to ensure no overlap in encoded values.

  Args:
    df: DataFrame with encoded categorical features.
    cat_features: List of categorical column names.
    numeric_features: List of numeric columns to consider for initial shift.

  Returns:
    Same DataFrame as input 'df'.
  """
  # Get initial shift from max value in base numeric columns
  offset = max(df[numeric_features].max().max(), 0)
  # Offset each categorical feature
  for f in cat_features:
    delta = len(df[f].unique())
    df[f] = df[f] + offset
    offset += delta
  return df


def get_unique_values(df: pd.DataFrame,
                      stratify_features: list[str]) -> list[str]:
  """Collect all unique values across features used for stratification.

  Args:
    df: DataFrame with encoded and processed features.
    stratify_features: List of feature names to collect values from.

  Returns:
    List of all unique values found across specified features.
  """
  values = []
  for col in stratify_features:
    unique_vals = df[col].astype(str).unique()
    values.extend(unique_vals)
  return values


def split_via_stratification(df: pd.DataFrame,
                             split_ratio: float = 0.5) -> SplittingResult:
  """Split users using stratified sampling.

  Split users while maintaining similar distributions of multiple features
  across test and control groups.

  Args:
    df: A DataFrame with users.
    split_ratio: A ratio of test and control groups.

  Returns:
    SplittingResult with test and control users,
    as well as metrics and distributions to assess the split quality.
  """
  # we tolerate duplicates in the input DF
  original_len = len(df)
  df = df.drop_duplicates(subset=['user'], keep='last')
  dedup_len = len(df)
  if dedup_len != original_len:
    logger.parent.warning(
        'Dataset passed to stratification contained %s duplicating rows',
        original_len - dedup_len)

  if not split_ratio:
    split_ratio = 0.5

  # Define features we'll never use for stratification
  exclude_cols = ['user']

  # Special feature that needs binning
  binning_cols = ['n_sessions', 'days_since_install']

  # Automatically detect numeric and categorical features
  numeric_features = df.drop(columns=exclude_cols + binning_cols).select_dtypes(
      include=['int64', 'float64']).columns.tolist()
  cat_features = df.drop(columns=exclude_cols + binning_cols).select_dtypes(
      include=['object', 'category']).columns.tolist()

  # do binning
  for col in binning_cols:
    bins = binsify(df, col)
    df[f'{col}_bins'] = np.searchsorted(bins, df[col].values, side='right') - 1
    numeric_features.append(f'{col}_bins')

  diagnostics_logger.debug('Detected numeric features: %s', numeric_features)
  diagnostics_logger.debug('Detected categorical features: %s', cat_features)

  # encode original DF: all categorical columns will be encoded as integers
  encoded, cat_features = make_encoding(
      df, exclude_cols=exclude_cols, all_cols=df.columns)

  encoded = offset_features(encoded, cat_features, numeric_features)
  # Define all features we want to stratify on
  # e.g. ['num_sessions_bins', 'days_since_install_bins', 'brand','src', 'osv']
  stratify_features = numeric_features + cat_features
  unique_values = get_unique_values(encoded, stratify_features)
  cols = df.drop(columns=['user', 'n_sessions', 'days_since_install']).columns
  # creates a new 'labels' column where each row contains a list of
  # all feature values for that row, converted to strings
  encoded['labels'] = encoded.apply(
      lambda x: list(map(str, [x[c] for c in cols])), axis=1)

  encoded_ids = stratify(
      data=encoded.labels.values,
      classes=list(map(str, unique_values)),
      ratio=split_ratio)

  # split DF onto two DF with test and control users
  test_ids = encoded_ids[0]
  users_test = encoded.loc[test_ids, ['user']]
  users_control = encoded.loc[~(encoded['user'].isin(users_test['user'])),
                              ['user']]
  logger.debug('Test group size: %s, control group size: %s', len(users_test),
               len(users_control))

  # Calculate validation metrics
  metrics = get_split_metrics(
      df,
      users_test,
      users_control,
      numeric_features=numeric_features,
      categorical_features=cat_features)

  # Get distribution data
  distributions = prepare_distribution_data(df, users_test, users_control,
                                            numeric_features, cat_features)

  diagnostics_logger.debug(metrics)

  return SplittingResult(
      users_test=users_test,
      users_control=users_control,
      metrics=metrics,
      distributions=distributions)


def get_split_metrics(
    df: pd.DataFrame, users_test: pd.DataFrame, users_control: pd.DataFrame,
    numeric_features: list[str],
    categorical_features: list[str]) -> dict[str, FeatureMetrics]:
  """Calculate metrics comparing test and control group distributions.

  Uses chi-square test for categorical features and calculates mean/std ratios
  for numeric features to validate quality of the split.

  Args:
    df: Original DataFrame with all users.
    users_test: DataFrame with test group user IDs.
    users_control: DataFrame with control group user IDs.
    numeric_features: List of numeric column names to compare.
    categorical_features: List of categorical column names to compare.

  Returns:
    dict: Metrics for each feature containing:
      For categorical features:
        - p_value: Chi-square test p-value
        - js_divergence: Jensen-Shannon divergence between distributions
        - proportion_diffs: differences for each category
        - max_diff: max difference
      For numeric features:
        - mean_ratio: Ratio of test/control means
        - std_ratio: Ratio of test/control standard deviations
        - ks_statistic: Kolmogorov-Smirnov (KS) test
        - p_value: KS-test p-value
      - warnings: Dict of warnings detected
  """
  test_df = df[df['user'].isin(users_test['user'])]
  control_df = df[df['user'].isin(users_control['user'])]

  metrics = {}

  # Numeric features
  for feat in numeric_features:
    warnings = {}

    # Mean and std ratios
    test_mean = test_df[feat].mean()
    control_mean = control_df[feat].mean()
    mean_ratio = (
        test_mean / control_mean if pd.notnull(test_mean) and
        pd.notnull(control_mean) and control_mean != 0 else None)
    if mean_ratio and abs(mean_ratio - 1) > 0.1:
      warnings['mean_ratio'] = f'Mean differs by {abs(mean_ratio-1)*100:.1f}%'

    test_std = test_df[feat].std()
    control_std = control_df[feat].std()
    std_ratio = (
        test_std / control_std if pd.notnull(test_std) and
        pd.notnull(control_std) and control_std != 0 else None)
    if std_ratio and abs(std_ratio - 1) > 0.2:
      warnings['std_ratio'] = (
          f'Standard deviation differs by {abs(std_ratio-1)*100:.1f}%')

    # KS test: The Kolmogorov-Smirnov (KS) test is comparing two distributions
    # by looking at their cumulative distribution functions (CDFs).
    # The test calculates maximum distance between these CDFs.
    try:
      ks_stat, p_neq = stats.ks_2samp(
          test_df[feat], control_df[feat], alternative='two-sided')

      # p-values < 0.05 means we got statistically significant difference
      if p_neq < 0.05:
        warnings['ks_test'] = (
            f'Distributions are significantly different (p={p_neq:.3f})')
        # ks_stat shows how big the difference is
        if ks_stat > 0.1:
          warnings['ks_statistic'] = (
              f'Large difference in distributions: {ks_stat:.1%}')
    # pylint: disable=bare-except
    except:
      ks_stat = p_neq = None

    metrics[feat] = FeatureMetrics(
        mean_ratio=float(mean_ratio) if mean_ratio is not None else None,
        std_ratio=float(std_ratio) if std_ratio is not None else None,
        ks_statistic=float(ks_stat) if ks_stat is not None else
        None,  # the maximum absolute difference between CDFs
        p_value=float(p_neq) if p_neq is not None else
        None,  # Tests if distributions are different in any way
        warnings=warnings if warnings else None)

  # Categorical features
  for feat in categorical_features:
    warnings = {}

    # Get distributions
    test_dist = test_df[feat].value_counts(normalize=True)
    control_dist = control_df[feat].value_counts(normalize=True)

    # Calculate differences for each value
    all_values = sorted(set(test_dist.index) | set(control_dist.index))
    proportion_diffs = {}
    for val in all_values:
      test_prop = test_dist.get(val, 0)
      control_prop = control_dist.get(val, 0)
      diff = test_prop - control_prop
      if diff != 0:
        proportion_diffs[str(val)] = {
            'test_pct': test_prop * 100,
            'control_pct': control_prop * 100,
            'diff_pct': diff * 100
        }

    # Calculate maximum difference
    max_diff = max(
        abs(test_dist.get(val, 0) - control_dist.get(val, 0))
        for val in all_values)

    if max_diff > 0.05:
      max_diff_val = max(
          all_values,
          key=lambda x: abs(test_dist.get(x, 0) - control_dist.get(x, 0)))
      warnings['distribution'] = (
          f'Large distribution difference for value {max_diff_val}: '
          f'test={test_dist.get(max_diff_val, 0)*100:.1f}%, '
          f'control={control_dist.get(max_diff_val, 0)*100:.1f}%')

    # Align distributions for statistical tests
    test_aligned = pd.Series(0, index=all_values)
    control_aligned = pd.Series(0, index=all_values)
    test_aligned[test_dist.index] = test_dist
    control_aligned[control_dist.index] = control_dist

    # Scale control distribution to match test total
    test_total = test_aligned.sum()
    control_total = control_aligned.sum()
    control_aligned = control_aligned * (test_total / control_total)

    # Chi-square test
    try:
      chi2, p_value = stats.chisquare(test_aligned, control_aligned)
      if p_value < 0.05:
        warnings['p_value'] = (
            f'Significantly different distributions (p={p_value:.3f})')
    # pylint: disable=bare-except
    except:
      p_value = None

    # JS (Jensen-Shannon) divergence
    # - is a measure of similarity between two distributions:
    # * 0 indicates identical distributions
    # * Values > 0.1 suggest substantial differences
    # * Higher values indicate greater distributional differences
    try:
      test_prop = test_dist / test_dist.sum()
      control_prop = control_dist / control_dist.sum()
      js_div = stats.entropy(test_prop, control_prop)
      if js_div > 0.1:
        warnings['js_divergence'] = f'High JS divergence: {js_div:.3f}'
    # pylint: disable=bare-except
    except:
      js_div = None

    metrics[feat] = FeatureMetrics(
        proportion_diffs=proportion_diffs,
        max_diff=float(max_diff) if max_diff is not None else None,
        p_value=float(p_value) if p_value is not None else None,
        js_divergence=float(js_div) if js_div is not None else None,
        warnings=warnings if warnings else None)

  return metrics


def prepare_distribution_data(
    df: pd.DataFrame, users_test: pd.DataFrame, users_control: pd.DataFrame,
    numeric_features: list[str],
    categorical_features: list[str]) -> list[DistributionData]:
  """Prepare feature distribution data for visualization and analysis.

  Args:
    df: Original DataFrame with all users.
    users_test: DataFrame with test group user IDs.
    users_control: DataFrame with control group user IDs.
    numeric_features: List of numeric column names to compare.
    categorical_features: List of categorical column names to compare.

  Returns:
    List of DistributionData
  """
  test_df = df[df['user'].isin(users_test['user'])]
  control_df = df[df['user'].isin(users_control['user'])]

  distributions = []

  for feat in numeric_features:
    test_dist = test_df[feat].value_counts(normalize=True)
    control_dist = control_df[feat].value_counts(normalize=True)

    # Get all actual bin numbers that exist in data
    all_bins = sorted(set(test_dist.index) | set(control_dist.index))

    # Create aligned distributions using actual bin numbers
    test_aligned = pd.Series(0, index=all_bins)
    control_aligned = pd.Series(0, index=all_bins)
    test_aligned[test_dist.index] = test_dist
    control_aligned[control_dist.index] = control_dist

    # Get bin labels for these bin numbers
    original_col = feat.replace('_bins', '')
    bins = binsify(df, original_col)
    bin_labels = []
    for i in all_bins:
      if i == 1:  # First bin
        label = f'0-{bins[1]:.0f}'
      elif i == len(bins):  # Last bin
        label = f'≥{bins[-1]:.0f}'
      else:  # Middle bins
        label = f'{bins[i-1]:.0f}-{bins[i]:.0f}'
      bin_labels.append(label)

    distributions.append(
        DistributionData(
            feature_name=feat,
            is_numeric=True,
            categories=bin_labels,  # human-readable bin labels
            test_distribution=test_aligned.tolist(),
            control_distribution=control_aligned.tolist()))

  for feat in categorical_features:
    test_dist = test_df[feat].value_counts(normalize=True)
    control_dist = control_df[feat].value_counts(normalize=True)

    # Get all values and align distributions
    all_values = sorted(set(test_dist.index) | set(control_dist.index))
    test_aligned = pd.Series(0, index=all_values)
    control_aligned = pd.Series(0, index=all_values)
    test_aligned[test_dist.index] = test_dist
    control_aligned[control_dist.index] = control_dist

    distributions.append(
        DistributionData(
            feature_name=feat,
            is_numeric=False,
            categories=[str(x) for x in all_values],
            test_distribution=test_aligned.tolist(),
            control_distribution=control_aligned.tolist()))

  return distributions
