/*
 Copyright 2024 Google LLC

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

export enum AudienceMode {
  OFF = 'off',
  TEST = 'test',
  PROD = 'prod',
}

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
export interface CampaignInfo {
  campaign_id: string;
  customer_id: string;
  campaign_name: string;
}
export interface UserlistAssignementData {
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
export interface AudienceWithLog extends AudienceInfo {
  log?: AudienceLog[];
  conversions?: Conversions;
  ads?: {
    campaigns: CampaignInfo[];
    adgroups: UserlistAssignementData[];
    tree: AdsTreeData[];
  };
}
export interface AudienceLog {
  date: Date | string;
  test_user_count: number;
  control_user_count: number;
  uploaded_user_count: number;
  new_test_user_count: number;
  new_control_user_count: number;
  total_test_user_count: number;
  total_control_user_count: number;

  job_status: string;
  job_failure: string;
}
export interface Conversions {
  data: ConversionsData[];
  start_date: string;
  end_date: string;
  pval: number | undefined;
  ads_metrics?: Record<string, AdsMetric[]>;
}
export interface ConversionsData {
  date: string;
  /**
   * Conversion rate of the test group.
   */
  cr_test: number;
  /**
   * Conversion rate of the control group.
   */
  cr_control: number;
  /**
   * Cummulative number of conversions of the test group.
   */
  cum_test_regs: number;
  /**
   * Cummulative number of conversions of the control group.
   */
  cum_control_regs: number;
}
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
export const useAudiencesStore = defineStore('audiences', () => {
  const audiences = ref([] as AudienceInfo[]);
  const deletedAudiences = ref([] as AudienceInfo[]);

  return {
    audiences,
    deletedAudiences,
    loadAudiences,
    removeAudience,
  };
});
