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
import unittest
from server.models import FeatureMetrics
import numpy as np


class TestFeatureMetrics(unittest.TestCase):
  """Tests for FeatureMetrics"""

  def test_initialization_with_none(self):
    metrics = FeatureMetrics()
    self.assertIsNone(metrics.mean_ratio)
    self.assertIsNone(metrics.std_ratio)
    self.assertIsNone(metrics.ks_statistic)
    self.assertIsNone(metrics.p_value)
    self.assertIsNone(metrics.js_divergence)
    self.assertIsNone(metrics.proportion_diffs)
    self.assertIsNone(metrics.max_diff)
    self.assertIsNone(metrics.warnings)

  def test_initialization_with_numpy_floats(self):
    metrics = FeatureMetrics(
        mean_ratio=np.float64(1.0350069960176516),
        std_ratio=np.float64(1.0384476706721888),
        ks_statistic=np.float64(0.037189599766286885),
        p_value=np.float64(0.8702536828710294),
        js_divergence=np.float64(0.0123456),
        max_diff=np.float64(1.2345678901))
    self.assertIsInstance(metrics.mean_ratio, float)
    self.assertEqual(metrics.mean_ratio, 1.0350069960176516)
    self.assertIsInstance(metrics.std_ratio, float)
    self.assertEqual(metrics.std_ratio, 1.0384476706721888)
    self.assertIsInstance(metrics.ks_statistic, float)
    self.assertEqual(metrics.ks_statistic, 0.037189599766286885)
    self.assertIsInstance(metrics.p_value, float)
    self.assertEqual(metrics.p_value, 0.8702536828710294)
    self.assertIsInstance(metrics.js_divergence, float)
    self.assertEqual(metrics.js_divergence, 0.0123456)
    self.assertIsInstance(metrics.max_diff, float)
    self.assertEqual(metrics.max_diff, 1.2345678901)
