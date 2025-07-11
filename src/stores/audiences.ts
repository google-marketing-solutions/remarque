/*
 Copyright 2023-2025 Google LLC

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
import { ref } from 'vue';
import { defineStore } from 'pinia';
import { getApi } from 'boot/axios';

/**
 * An audience mode.
 */
export enum AudienceMode {
  OFF = 'off',
  TEST = 'test',
  PROD = 'prod',
}

/**
 * An audience description.
 */
export interface AudienceInfo {
  name: string;
  app_id: string;
  countries: string[];
  events_include: string[];
  events_exclude: string[];
  days_ago_start: number;
  days_ago_end: number;
  user_list: string;
  mode: AudienceMode;
  query: string;
  ttl: number;
  split_ratio: number | undefined | null;
  created?: string | undefined;
  isNew?: boolean;
  isChanged?: boolean;
}

/**
 * A Ads campaign description.
 */
export interface CampaignInfo {
  campaign_id: string;
  customer_id: string;
  campaign_name: string;
}

/**
 * A description of an audience assignment to ads entities (adgroup/campaign).
 */
export interface UserlistAssignmentData {
  customer_id: string;
  customer_name: string;
  campaign_id: string;
  campaign_name: string;
  campaign_status: string;
  campaign_start_date: string;
  campaign_end_date: string;
  ad_group_id: string;
  ad_group_name: string;
  ad_group_status: string;
  user_list_id: string;
  user_list_name: string;
  user_list_description: string;
  user_list_size_for_search: number;
  user_list_size_for_display: number;
  user_list_eligible_for_search: number;
  user_list_eligible_for_display: number;
  customer_link: string;
  campaign_link: string;
  ad_group_link: string;
  user_list_link: string;
}

/**
 * Association level for a user list.
 */
export type AdsTreeNodeType =
  | 'customer'
  | 'campaign'
  | 'ad_group'
  | 'user_list';

export interface AdsTreeData {
  label: string;
  status?: string;
  type: AdsTreeNodeType;
  id: string;
  selected?: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  info?: Record<string, any>;
  children?: AdsTreeData[];
}
/**
 * Extended description of an audience with logs, conversions and ads metrics.
 */
export interface AudienceWithLog extends AudienceInfo {
  log?: AudienceLog[];
  conversions?: Conversions;
  ads?: {
    campaigns: CampaignInfo[];
    adgroups: UserlistAssignmentData[];
    tree: AdsTreeData[];
  };
}

/**
 * An item of an audience log.
 */
export interface AudienceLog {
  date: Date | string;
  test_user_count: number;
  control_user_count: number;
  uploaded_user_count: number;
  failed_user_count: number;
  new_test_user_count: number;
  new_control_user_count: number;
  total_test_user_count: number;
  total_control_user_count: number;
  job: string;
  job_status: string;
  job_failure: string;
}

/**
 * An audience conversions and ads metrics.
 */
export interface Conversions {
  data: ConversionsData[];
  start_date: string;
  end_date: string;
  pval: number | undefined;
  pval_events: number | undefined;
  ads_metrics?: Record<string, AdsMetric[]>;
}
/**
 * Item of conversion history for an audience (of a day).
 */
export interface ConversionsData {
  date: string;
  /**
   * Cumulative number of users in the test group.
   */
  total_test_user_count: number;
  /**
   * Cumulative number of users in the control group.
   */
  total_control_user_count: number;
  /**
   * Cumulative number of sessions (session_start) of the test group.
   */
  test_session_count: number;
  /**
   * Cumulative number of sessions (session_start) of the control group.
   */
  control_session_count: number;
  /**
   * Cumulative number of converted users in the test group.
   */
  cum_test_users: number;
  /**
   * Cumulative number of converted users in the control group.
   */
  cum_control_users: number;
  /**
   * Cumulative number of conversion events in the test group.
   */
  cum_test_events: number;
  /**
   * Cumulative number of conversion events in the control group.
   */
  cum_control_events: number;
  /**
   * Cumulative conversion value of the test group.
   */
  cum_test_conv_value: number;
  /**
   * Cumulative conversion value of the control group.
   */
  cum_control_conv_value: number;

  // the metrics below are calculated on the client-side.
  /**
   * Conversion rate for users of the test group.
   */
  cr_test: number;
  /**
   * Conversion rate for users of the control group.
   */
  cr_control: number;
  /**
   * Avg event count per user in the test group.
   */
  events_per_user_test: number;
  /**
   * Avg event count per user in the control group.
   */
  events_per_user_control: number;
  /**
   * Avg event count per session in the test group.
   */
  events_per_session_test: number;
  /**
   * Avg event count per session in the control group.
   */
  events_per_session_control: number;
  /**
   * Value per conversion event in the test group.
   */
  value_per_event_test: number;
  /**
   * Value per conversion event in the control group.
   */
  value_per_event_control: number;
  /**
   * Value per converted user in the test group.
   */
  value_per_user_test: number;
  /**
   * Value per converted user in the control group.
   */
  value_per_user_control: number;
}

/**
 * An item of Ads metrics of an audience (for a day).
 */
export interface AdsMetric {
  campaign: string;
  date: string;
  unique_users: number;
  clicks: number;
  average_impression_frequency_per_user?: number;
}

interface IAudienceStore {
  audiences: AudienceInfo[];
  deletedAudiences: AudienceInfo[];
}

/**
 * Response type for 'audiences' endpoint.
 */
interface AudiencesLoadResponse {
  results: AudienceInfo[];
}

async function loadAudiences(this: IAudienceStore) {
  const res = await getApi<AudiencesLoadResponse>('audiences');
  this.audiences = res.data.results;
  return this.audiences;
}

function removeAudience(this: IAudienceStore, name: string) {
  const idx = this.audiences.findIndex(
    (val: AudienceInfo) => val.name === name,
  );
  if (idx >= 0) {
    const deleted = this.audiences.splice(idx, 1);
    this.deletedAudiences.push(...deleted);
  }
}

function getAudience(this: IAudienceStore, name: string) {
  return this.audiences.find((audience) => audience.name === name);
}

export const useAudiencesStore = defineStore('audiences', () => {
  const audiences = ref([] as AudienceInfo[]);
  const deletedAudiences = ref([] as AudienceInfo[]);

  return {
    audiences,
    deletedAudiences,
    loadAudiences,
    removeAudience,
    getAudience,
  };
});
