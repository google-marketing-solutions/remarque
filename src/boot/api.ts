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
import {
  AdsMetric,
  AudienceInfo,
  AudienceLog,
  Conversions,
  ConversionsData,
  UserlistAssignmentData,
} from 'src/stores/audiences';

/**
 * Feature distribution from splitting
 */
export interface Distributions {
  feature_name: string;
  is_numeric: boolean;
  // For numeric features
  test_values?: number[];
  control_values?: number[];
  // For categorical features
  categories?: string[];
  test_distribution?: number[];
  control_distribution?: number[];
}

/**
 * Result of processing of one audience (via 'process').
 */
export interface AudienceProcessResult {
  job_resource_name: string;
  test_user_count: number;
  control_user_count: number;
  failed_user_count: number;
  uploaded_user_count: number;
  new_test_user_count: number;
  new_control_user_count: number;
  total_test_user_count: number;
  total_control_user_count: number;
  distributions: Distributions[];
  metrics: Map<string, Record<string, string>>;
}
/**
 * Result of audience(s) processing.
 */
export type AudiencesProcessResult = Record<string, AudienceProcessResult>;
/**
 * Response type for 'process' and 'ads/upload' endpoints.
 */
export interface AudiencesProcessResponse {
  result: AudiencesProcessResult;
}

/**
 * Conversions for an audience.
 */
export interface AudienceConversionsResult {
  conversions: ConversionsData[];
  ads_metrics: AdsMetric[];
  date_start: string;
  date_end: string;
  pval: number | undefined;
  pval_events: number | undefined;
  chi: number | undefined;
}
/**
 * Response type for 'conversions/' endpoint.
 */
export interface AudienceConversionsResponse {
  results: Record<string, AudienceConversionsResult>;
}
/**
 * Response type for 'conversions/query' endpoint.
 */
export interface AudienceConversionsQueryResponse {
  query: string;
  date_start: string;
  date_end: string;
}

/**
 * Extended status for an audience (with logs and conversions).
 */
export interface AudienceStatusResult extends AudienceInfo {
  log?: AudienceLog[];
  conversions?: Conversions;
  campaigns: UserlistAssignmentData[];
}
/**
 * Response type for 'audiences/status' endpoint.
 */
export interface AudiencesStatusResponse {
  result: Record<string, AudienceStatusResult>;
}
/**
 * Response type for 'audiences/recalculate_log' endpoint.
 */
export interface AudiencesLogRebuildResponse {
  result: Record<string, AudienceLog[]>;
}
