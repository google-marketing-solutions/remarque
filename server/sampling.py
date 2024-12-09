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

import numpy as np
import pandas as pd
import warnings
from scipy import stats
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
import math
from logger import logger
from models import FeatureMetrics, DistributionData, SplittingResult

warnings.filterwarnings('ignore')

logger = logger.getChild('sampling')


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
  label_combinations = {}
  for i, labels in enumerate(data):
    key = tuple(sorted(labels))
    label_combinations[key] = label_combinations.get(key, 0) + 1

  # For each subset we want, the set of sample-ids which should end up in it
  stratified_data_ids = [set(), set()]

  # For each sample in the data set
  while size > 0:
    # Find label with fewest remaining samples
    label = min((l for l, data in per_label_data.items() if data),
                key=lambda l: len(per_label_data[l]))

    # Process all samples with this label
    while per_label_data[label]:
      # Take one sample
      current_id = per_label_data[label].pop()
      # Find which subset (test/control) needs this label most -
      # So it's a two-level decision:
      #   1. First try to balance specific feature value
      #   2. If tied, try to balance overall group sizes
      #   3. If still tied, random choice
      if per_label_subset_sizes[label][0] > per_label_subset_sizes[label][1]:
        subset = 0  # test group needs this label more
      elif per_label_subset_sizes[label][0] < per_label_subset_sizes[label][1]:
        subset = 1  # control group needs this label more
      else:
        # If tied on this label, check overall group sizes
        if subset_sizes[0] > subset_sizes[1]:
          subset = 0  # test group needs more samples
        elif subset_sizes[0] < subset_sizes[1]:
          subset = 1  # control group needs more samples
        else:
          # completely tied, choose randomly
          subset = np.random.choice([0, 1])

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
    df[f] = df[f].apply(lambda x: x + offset)
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
    and metrics and distributions to assess the split quality.
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
    df[f'{col}_bins'] = np.searchsorted(bins, df[col].values)
    numeric_features.append(f'{col}_bins')

  logger.debug('Detected numeric features: %s', numeric_features)
  logger.debug('Detected categorical features: %s', cat_features)

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

  logger.debug(metrics)

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
        - warnings: Dict of warnings if distributions differ significantly
      For numeric features:
        - mean_ratio: Ratio of test/control means
        - std_ratio: Ratio of test/control standard deviations
        - warnings: Dict of warnings if ratios exceed thresholds
  """
  test_df = df[df['user'].isin(users_test['user'])]
  control_df = df[df['user'].isin(users_control['user'])]

  metrics = {}

  # For numeric features - compare means and standard deviations
  for feat in numeric_features:
    warnings = {}
    feat_metrics = FeatureMetrics()
    metrics[feat] = feat_metrics

    # mean and std deviations
    mean_ratio = test_df[feat].mean() / control_df[feat].mean()
    std_ratio = test_df[feat].std() / control_df[feat].std()
    feat_metrics.mean_ratio = mean_ratio
    feat_metrics.std_ratio = std_ratio

    # The Kolmogorov-Smirnov (KS) test is a statistical test that
    # determines if two samples come from the same distribution
    statistic, p_neq = stats.ks_2samp(
        test_df[feat], control_df[feat], alternative='two-sided')
    _, p_gt = stats.ks_2samp(
        test_df[feat], control_df[feat], alternative='greater')
    _, p_lt = stats.ks_2samp(
        test_df[feat], control_df[feat], alternative='less')

    feat_metrics.ks_statistic = statistic
    feat_metrics.ks_pvalue_neq = p_neq
    feat_metrics.ks_pvalue_gt = p_gt
    feat_metrics.ks_pvalue_lt = p_lt

    # Warning if mean differs by more than 10%
    if pd.notnull(mean_ratio) and abs(mean_ratio - 1) > 0.1:
      warnings['mean_ratio'] = f'Mean differs by {abs(mean_ratio-1)*100:.1f}%'
    # Warning if std differs by more than 20%
    if pd.notnull(std_ratio) and abs(std_ratio - 1) > 0.2:
      warnings['std_ratio'] = (
          f'Standard deviation differs by {abs(std_ratio-1)*100:.1f}%')
    if p_neq < 0.05:
      warnings['ks_test'] = (
          f'Significantly different distributions (KS p={p_neq:.3f})')

    if warnings:
      feat_metrics.warnings = warnings

  # For categorical features - compare value distributions
  for feat in categorical_features:
    warnings = {}
    feat_metrics = FeatureMetrics()
    metrics[feat] = feat_metrics

    # Get distributions including nulls (nulls will be counted as a category)
    test_dist = test_df[feat].value_counts(dropna=False)
    control_dist = control_df[feat].value_counts(dropna=False)

    # Ensure both distributions have same categories
    categories = set(test_dist.index) | set(control_dist.index)
    test_aligned = pd.Series(0, index=categories)
    control_aligned = pd.Series(0, index=categories)
    test_aligned[test_dist.index] = test_dist
    control_aligned[control_dist.index] = control_dist

    # Calculate chi-square test with raw counts
    try:
      # Chi-square test on raw counts
      chi2, p_value = stats.chisquare(test_aligned, control_aligned)
      feat_metrics.p_value = p_value
      if p_value < 0.05:
        warnings[
            'p_value'] = f'Significantly different distributions {p_value:.3f}'
    except BaseException as e:
      feat_metrics.p_value = None
      warnings['p_value'] = 'Unable to compare distributions: ' + str(e)

    # Jensen-Shannon divergence (similarity measure)
    # js_div = stats.entropy(test_dist, control_dist)
    # feat_metrics['js_divergence'] = js_div
    # if js_div > 0.1:
    #   warnings['js_divergence'] = f'High JS divergence: {js_div:.3f}'
    try:
      # JS divergence needs probability distributions
      test_prop = test_aligned / test_aligned.sum()
      control_prop = control_aligned / control_aligned.sum()
      js_div = stats.entropy(test_prop, control_prop)
      feat_metrics.js_divergence = js_div
      if js_div > 0.1:
        warnings['js_divergence'] = f'High JS divergence: {js_div:.3f}'
    except BaseException as e:
      feat_metrics.js_divergence = None
      warnings['js_divergence'] = 'Unable to calculate JS divergence: ' + str(e)

    if warnings:
      feat_metrics.warnings = warnings

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
    List of DistributionData per feature containing:
      feature_name: Name of the feature.
      is_numeric: Whether feature is numeric or categorical.
      categories: Category names for categorical or bin centers for numeric.
      bin_edges: Bin edges for numeric features (None for categorical).
      test_distribution: Distribution of values in test group (proportions).
      control_distribution: Distribution of values in control group.
  """
  test_df = df[df['user'].isin(users_test['user'])]
  control_df = df[df['user'].isin(users_control['user'])]

  distributions = []

  # For numeric features
  for feat in numeric_features:
    # Calculate histogram bins
    all_values = df[feat].values
    min_val = all_values.min()
    max_val = all_values.max()
    n_bins = 30
    bin_edges = np.linspace(min_val, max_val, n_bins + 1)
    bin_centers = [(bin_edges[i] + bin_edges[i + 1]) / 2
                   for i in range(len(bin_edges) - 1)]

    # Calculate distributions
    test_hist, _ = np.histogram(test_df[feat], bins=bin_edges, density=True)
    control_hist, _ = np.histogram(
        control_df[feat], bins=bin_edges, density=True)

    distributions.append(
        DistributionData(
            feature_name=feat,
            is_numeric=True,
            categories=bin_centers,
            bin_edges=bin_edges.tolist(),
            test_distribution=test_hist.tolist(),
            control_distribution=control_hist.tolist()))

  # For categorical features
  for feat in categorical_features:
    test_dist = test_df[feat].value_counts(normalize=True)
    control_dist = control_df[feat].value_counts(normalize=True)

    # Get all categories
    all_categories = sorted(set(test_dist.index) | set(control_dist.index))

    distributions.append(
        DistributionData(
            feature_name=feat,
            is_numeric=False,
            categories=all_categories,
            bin_edges=[],
            test_distribution=[test_dist.get(cat, 0) for cat in all_categories],
            control_distribution=[
                control_dist.get(cat, 0) for cat in all_categories
            ]))

  return distributions


def poisson_rate_test(event_count_test: int, event_count_control: int,
                      exposure_test: int,
                      exposure_control: int) -> tuple[float, float]:
  """Perform a Poisson rate test for comparing event rates between two groups.

  Args:
    event_count_test: Number of events in the test group.
    event_count_control: Number of events in the control group.
    exposure_test: Total exposure (e.g., user-days) in the test group.
    exposure_control: Total exposure (e.g., user-days) in the control group.
  Returns:
    A tuple of test statistic, p-value
  """
  rate_ratio = (event_count_test / exposure_test) / (
      event_count_control / exposure_control)
  se_log_rate_ratio = ((1 / event_count_test) + (1 / event_count_control))**0.5
  z_statistic = abs(math.log(rate_ratio) / se_log_rate_ratio)
  p_value = 2 * (1 - stats.norm.cdf(z_statistic))  # Two-tailed test

  return z_statistic, p_value
