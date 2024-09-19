/*
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
 */

import format from 'date-fns/format';
import { isNumber, isDate, isArray, isString } from 'lodash';

export function formatArray(val: unknown) {
  if (isArray(val)) {
    return val.join(', ');
  }
  return val;
}

export function formatDate(val: unknown, includeTime = false) {
  if (!val) return '';
  if (typeof val === 'string') {
    val = new Date(val);
  }
  if (isDate(val) || isNumber(val)) {
    if (includeTime) {
      return format(val, 'yyyy.MM.dd HH:mm:ss');
    }
    return format(val, 'yyyy.MM.dd');
  }
  return val;
}

export function formatFloat(val: unknown, digits = 5) {
  if (isString(val)) {
    val = Number(val);
  }
  if (isNumber(val)) {
    return val >= 0 ? val.toFixed(digits) : '-';
  }
  return val;
}

export function isFinite(value: unknown) {
  return typeof value === 'number' && Number.isFinite(value);
}

export function assertIsError(e: unknown): asserts e is Error {
  if (!(e instanceof Error)) throw new Error('e is not an Error');
}
