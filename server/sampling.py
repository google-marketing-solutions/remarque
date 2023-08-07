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

import warnings
warnings.filterwarnings('ignore')
#import sys
import numpy as np
import pandas as pd
#import hashlib
#import uuid

from copy import deepcopy
import scipy.stats as ss
from scipy.stats import ks_2samp
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
import seaborn as sns; sns.set()
import matplotlib.pyplot as plt
from matplotlib.ticker import NullFormatter

import datetime as dt

from logger import logger


def makeEncoding(df: pd.DataFrame, exclude_cols: list[str], all_cols, encoder=None):
    """TODO:
    """
    _df = df.copy()
    dtypes_dct = dict(df.dtypes.to_frame('dtypes').reset_index().values)

    # numerical_ix = _df.drop(exclude_cols, axis=1).select_dtypes(include=['int64', 'float64']).columns.values.tolist()
    cat_ix = _df.drop(exclude_cols, axis=1).select_dtypes(include=['object']).columns.values.tolist()
    #print(cat_ix)
    # t = [('num', MinMaxScaler(), numerical_ix), ('cat', OrdinalEncoder(), cat_ix)]
    t = [('cat', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-99), cat_ix)]
    col_transform = ColumnTransformer(transformers=t, remainder='passthrough')
    if encoder is None:
        encoder = col_transform.fit(_df)
    res = encoder.transform(_df)
    part_cols = [col for col in all_cols if col not in cat_ix]
    reorder_df = pd.DataFrame(res, columns=cat_ix + part_cols).astype(dtypes_dct)

    reorder_df[cat_ix] = reorder_df[cat_ix].astype(int)

    return reorder_df[all_cols], encoder, cat_ix


def stratify(data, classes, ratios, one_hot=False):
    """Stratifying procedure.
    Algorithm is from: https://vict0rs.ch/2018/05/24/sample-multilabel-dataset/

    data is a list of lists: a list of labels, for each sample.
        Each sample's labels should be ints, if they are one-hot encoded, use one_hot=True

    classes is the list of classes each label can take

    ratios is a list, summing to 1, of how the dataset should be split

    """
    # one-hot decoding
    if one_hot:
        temp = [[] for _ in range(len(data))]
        indexes, values = np.where(np.array(data).astype(int) == 1)
        for k, v in zip(indexes, values):
            temp[k].append(v)
        data = temp

    # Organize data per label: for each label l, per_label_data[l] contains the list of samples
    # in data which have this label
    per_label_data = {c: set() for c in classes}
    for i, d in enumerate(data):
        for l in d:
            per_label_data[l].add(i)

    # number of samples
    size = len(data)

    # In order not to compute lengths each time, they are tracked here.
    subset_sizes = [r * size for r in ratios]
    target_subset_sizes = deepcopy(subset_sizes)
    per_label_subset_sizes = {
        c: [r * len(per_label_data[c]) for r in ratios]
        for c in classes
    }

    # For each subset we want, the set of sample-ids which should end up in it
    stratified_data_ids = [set() for _ in range(len(ratios))]

    # For each sample in the data set
    while size > 0:
        # Compute |Di|
        lengths = {
            l: len(label_data)
            for l, label_data in per_label_data.items()
        }
        try:
            # Find label of smallest |Di|
            label = min(
                {k: v for k, v in lengths.items() if v > 0}, key=lengths.get
            )
        except ValueError:
            # If the dictionary in `min` is empty we get a Value Error.
            # This can happen if there are unlabeled samples.
            # In this case, `size` would be > 0 but only samples without label would remain.
            # "No label" could be a class in itself: it's up to you to format your data accordingly.
            break
        current_length = lengths[label]

        # For each sample with label `label`
        while per_label_data[label]:
            # Select such a sample
            current_id = per_label_data[label].pop()

            subset_sizes_for_label = per_label_subset_sizes[label]
            # Find argmax clj i.e. subset in greatest need of the current label
            largest_subsets = np.argwhere(
                subset_sizes_for_label == np.amax(subset_sizes_for_label)
            ).flatten()

            if len(largest_subsets) == 1:
                subset = largest_subsets[0]
            # If there is more than one such subset, find the one in greatest need
            # of any label
            else:
                largest_subsets = np.argwhere(
                    subset_sizes == np.amax(subset_sizes)
                ).flatten()
                if len(largest_subsets) == 1:
                    subset = largest_subsets[0]
                else:
                    # If there is more than one such subset, choose at random
                    subset = np.random.choice(largest_subsets)

            # Store the sample's id in the selected subset
            stratified_data_ids[subset].add(current_id)

            # There is one fewer sample to distribute
            size -= 1
            # The selected subset needs one fewer sample
            subset_sizes[subset] -= 1

            # In the selected subset, there is one more example for each label
            # the current sample has
            for l in data[current_id]:
                per_label_subset_sizes[l][subset] -= 1

            # Remove the sample from the dataset, meaning from all per_label dataset created
            for l, label_data in per_label_data.items():
                if current_id in label_data:
                    label_data.remove(current_id)

    # Create the stratified dataset as a list of subsets, each containing the orginal labels
    stratified_data_ids = [sorted(strat) for strat in stratified_data_ids]
    stratified_data = [
        [data[i] for i in strat] for strat in stratified_data_ids
    ]

    # Return both the stratified indexes, to be used to sample the `features` associated with your labels
    # And the stratified labels dataset
    return stratified_data_ids, stratified_data


def get_diff_columns(train_df, test_df, show_plots=True, show_all=False,
                     threshold=0.1, alternative='two-sided', kde=False):
    """Use KS to estimate columns where distributions differ a lot from each other"""

    alternative = {'less': 'lt', 'greater': 'gt', 'two-sided': 'neq'}

    # Find the columns where the distributions are very different
    all_tests = {}
    for hypothesis, alias in alternative.items():
        diff_data = []
        for col in train_df.columns:
            statistic, pvalue = ks_2samp(
                train_df[col].values,
                test_df[col].values,
                alternative=hypothesis
            )
            #if pvalue <= 0.05 and np.abs(statistic) >= threshold:
            diff_data.append({'feature': col,
                              'p_' + alias: np.round(pvalue, 5),
                              'statistic_' + alias: np.round(np.abs(statistic), 3)
                             })
        all_tests[hypothesis] = diff_data

    # Put the differences into a dataframe
    all_dfs = []
    for _, diff_data in all_tests.items():
        diff_df = pd.DataFrame(diff_data)
        #diff_df.sort_values(by='statistic', ascending=False, inplace=True)
        all_dfs.append(diff_df)

    df = all_dfs[0]

    for i in range(len(all_dfs)-1):
        tmp_df = all_dfs[i+1]
        df = df.merge(tmp_df, how='left', on= 'feature')

    diff_df = df
    #print(diff_df.columns)

    if show_plots:
        # Let us see the distributions of these columns to confirm they are indeed different
        n_cols = 3
        if show_all:
            n_rows = int(len(diff_df) / 3)
        else:
            n_rows = 2
        _, axes = plt.subplots(n_rows, n_cols, figsize=(30, 3*n_rows))
        axes = [x for l in axes for x in l]

        # Create plots
        for i, (_, row) in enumerate(diff_df.iterrows()):
            if i >= len(axes):
                break
            if not kde:
                extreme = np.max(np.abs(train_df[row.feature].tolist() + test_df[row.feature].tolist()))
                train_df.loc[:, row.feature].hist(
                    ax=axes[i], alpha=0.5, label='Train', density=True,
                    bins=np.arange(0, extreme, 0.05)
                )
                test_df.loc[:, row.feature].hist(
                    ax=axes[i], alpha=0.5, label='Test', density=True,
                    bins=np.arange(0, extreme, 0.05)
                )
                axes[i].legend()
            else:
                sns.distplot(train_df.loc[:, row.feature], label='Train',
                             hist=False, kde=True, norm_hist=True, ax=axes[i])
                sns.distplot(test_df.loc[:, row.feature], label='Test',
                             hist=False, kde=True, norm_hist=True, ax=axes[i])
                axes[i].legend()

            axes[i].set_title('Two-Sided Test: Statistic = {}, p = {}'.format(row.statistic_neq, row.p_neq))
            axes[i].set_xlabel('{}'.format(row.feature))


        plt.tight_layout()
        plt.legend()
        plt.show()

    return diff_df


def binsify(df: pd.DataFrame, col: str, percentile=[0.2, 0.4, 0.6, 0.8]):
  bins = [0.0]
  p = sorted(list(set(np.quantile(df[col].values, percentile))))
  bins.extend(p)
  return bins


def offset_features(df: pd.DataFrame, cat_features: list[str]):
  SHIFT = max(df.days_since_install.max(), df.num_sessions_bins.max()) # UPDATE FEATURES
  for f in cat_features:
    offset = len(df[f].unique())
    df[f] = df[f].apply(lambda x: x + SHIFT)
    SHIFT += offset
  return df


def countClasses(df):
  n = []
  for c in ['num_sessions_bins', 'days_since_install', 'brand', 'src', 'osv']:
    n = n + list(df[c].unique())
  return n


def pred_data_for_test(df: pd.DataFrame, test_ids: list[str], test_frac):
  #df['ab_group'] = ['control' for x in range(df.shape[0])]
  #df.loc[test_ids, ['ab_group']] = 'test'
  #ts = int(dt.datetime.now().timestamp() * 1000000)
  #df['ts'] = [ts for x in range(df.shape[0])]

  users_test = df.loc[test_ids, ['user']]
  users_control = df.loc[~df['user'].isin(test_ids)]

  #ab_group = df[['user', 'ab_group', 'ts']]

  #customer_match.rename(columns={'user':'Mobile Device ID'}, inplace=True)
  #print('pred_data_for_test::', customer_match)

  #return users_test, ab_group, test_frac
  return users_test, users_control


def process_df(df: pd.DataFrame, frac_test=.5, max_test_share=30):

  bins = binsify(df, 'n_sessions')
  df['num_sessions_bins'] = np.searchsorted(bins, df['n_sessions'].values)

  encoded, encoder, cat_f = makeEncoding(df, exclude_cols=['user'], all_cols=df.columns)
  encoded = offset_features(encoded, cat_f)

  all_classes = countClasses(encoded)
  cols = df.drop(columns=['user', 'n_sessions']).columns
  encoded['labels'] = encoded.apply(lambda x: list(map(str, [x[c] for c in cols])), axis =1)

  encoded_ids, encoded_data = stratify(data=encoded.labels.values, classes=list(map(str,all_classes)), ratios=[frac_test, 1 - frac_test], one_hot=False)

  # for i in range(max_test_share):
  #   logger.debug(f'fraction test: {i}')
  #   frac_test += (i+1) / 100
  #   new_encoded_ids, new_encoded_data = stratify(data=encoded.labels.values, classes=list(map(str,all_classes)), ratios=[frac_test, 1 - frac_test], one_hot=False)
  #   test = encoded.loc[new_encoded_ids[0], cols]
  #   control = encoded.loc[new_encoded_ids[1], cols]
  #   stat_df = get_diff_columns(test, control, show_all = False, kde=True, show_plots=False)
  #   p_val = (sum(stat_df['p_gt'].values < .1) + sum(stat_df['p_lt'].values < .1) + sum(stat_df['p_neq'].values < .1))
  #   if p_val > 0:
  #     logger.debug(f'P-val sum of 3: {p_val}')
  #     break
  #   encoded_ids, encoded_data = new_encoded_ids, new_encoded_data

  # split DF onto two DF with test and control users
  test_ids = encoded_ids[0]
  users_test = encoded.loc[test_ids, ['user']]
  users_control = encoded.loc[~(encoded['user'].isin(users_test['user'])), ['user']]

  return users_test, users_control, frac_test


def do_sampling(df: pd.DataFrame):
  users_test, users_control, frac_test = process_df(df, max_test_share=16)
  #   frac_test - integer, test group fraction (the fraction of users in the test group of the total number of the users)
  user_count = df.shape[0]
  user_count_test = users_test.shape[0]
  user_count_control = users_control.shape[0]
  logger.info(f'Data sampling completed (user count: {user_count}, test count: {user_count_test}, control count: {user_count_control}, test fraction: {frac_test:2})')

  #test_table_name =f'{bq_dataset_id}.{audience_table}_test'
  #pandas_gbq.to_gbq(users_test, test_table_name, project_id, if_exists='replace')
  #control_table_name =f'{bq_dataset_id}.{audience_table}_control'
  #pandas_gbq.to_gbq(users_control, control_table_name, project_id, if_exists='replace')
  #logger.info(f'Sampled users exported to {test_table_name}/{control_table_name} tables')
  return users_test, users_control
