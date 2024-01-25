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

export function formatArray(val: any) {
  if (val && val.length) {
    return val.join(', ');
  }
  return val;
}

export function formatDate(val: Date, includeTime = false) {
  //let ret = val.toISOString().substring(0, '2000-01-01'.length);
  if (!val) return '';
  if (typeof val === 'string') {
    val = new Date(val);
  }
  if (includeTime) {
    return format(val, 'yyyy.MM.dd HH:mm:ss');
  }
  return format(val, 'yyyy.MM.dd');
}

export function formatFloat(val: number | undefined, digits = 5) {
  val = Number.parseFloat(<any>val);
  return val >= 0 ? val.toFixed(digits) : '-';
}

export function isFinite(value: any) {
  return typeof value == 'number' && Number.isFinite(value);
}
