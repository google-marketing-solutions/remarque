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

import { defineStore } from 'pinia';
import { getApi, setActiveTarget } from 'boot/axios';
import { useRouter } from 'vue-router';
import { ref, watch } from 'vue';

export enum AudienceMode {
  off = 'off',
  test = 'test',
  prod = 'prod',
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
  user_count?: number;
  mode: AudienceMode;
  query: string;
  ttl: number;
  split_ratio: number | undefined | null;
  created?: string | undefined;
  isNew?: boolean;
  isChanged?: boolean;
}

export interface ConfigTarget {
  name?: string;
  ads_client_id?: string;
  ads_client_secret?: string;
  ads_customer_id?: string;
  ads_developer_token?: string;
  ads_login_customer_id?: string;
  ads_refresh_token?: string;
  bq_dataset_id?: string;
  bq_dataset_location?: string;
  ga4_dataset?: string;
  ga4_project?: string;
  ga4_table?: string;
  ga4_loopback_window?: string;
  ga4_loopback_recreate?: boolean;
}

export enum States {
  Initial,
  Initialized,
  NotInitialized,
}

/*
export const configurationStore = defineStore('configuration', {
  state: () => ({
    state: States.Initial,
    debug: false,

    activeTarget: <string | undefined>'',

    name: '',
    name_org: '',
    is_new: false,

    targets: <ConfigTarget[]>[],

    project_id: '',
    bq_dataset_id: '',
    bq_dataset_location: '',
    ads_customer_id: '',
    ads_developer_token: '',
    ads_client_id: '',
    ads_client_secret: '',
    ads_refresh_token: '',
    ads_login_customer_id: '',

    ga4_project: '',
    ga4_dataset: '',
    ga4_table: '',

    days_ago_start: undefined, // TODO
    days_ago_end: undefined, // TODO

    stat: {
      app_ids: [] as string[],
      events: {} as Record<string, any[]>,
      countries: {} as Record<string, any[]>,
    },

    audiences: [] as AudienceInfo[],

    scheduled: undefined as boolean | undefined,
    schedule: '',
    schedule_timezone: '',
  }),
  actions: {
    removeAudience(name: string) {
      const idx = this.audiences.findIndex((val: any) => val.name === name);
      if (idx >= 0) {
        this.audiences.splice(idx, 1);
      }
    },
    initTarget(target: ConfigTarget) {
      this.name = target.name || '';
      this.name_org = target.name || '';
      this.bq_dataset_id = target.bq_dataset_id || '';
      this.bq_dataset_location = target.bq_dataset_location || '';
      this.ga4_project = target.ga4_project || '';
      this.ga4_dataset = target.ga4_dataset || '';
      this.ga4_table = target.ga4_table || '';
      this.ads_customer_id = target.ads_customer_id || '';
      this.ads_developer_token = target.ads_developer_token || '';
      this.ads_client_id = target.ads_client_id || '';
      this.ads_client_secret = target.ads_client_secret || '';
      this.ads_refresh_token = target.ads_refresh_token || '';
      this.ads_login_customer_id = target.ads_login_customer_id || '';
    },
    activateTarget(name: string | undefined) {
      const target = this.targets.find((t) => t.name === name);
      if (target) {
        this.activeTarget = name;
        this.initTarget(target);

        setActiveTarget(name);
        const router = useRouter();
        router.push({ query: { target: name } });
      } else {
        console.error(`activateTarget: Target with name ${name} not found`);
      }
    },
    async loadConfiguration() {
      try {
        console.log('loading configuration from server...');
        const res = await getApi('configuration');
        this.project_id = res.data.project_id;
        this.targets = res.data.targets || [];
        let target: ConfigTarget | undefined;

        const router = useRouter();
        const targetFromRoute = router.currentRoute.value.query.target;
        if (targetFromRoute) {
          this.activeTarget = <string>targetFromRoute;
        }
        if (this.activeTarget) {
          target = this.targets.find((t) => t.name === this.activeTarget);
        }
        if (!target && this.targets.length > 0) {
          target = this.targets[0];
          this.activeTarget = target.name;
        }
        if (!target) {
          target = {};
          this.activeTarget = '';
        }
        this.activateTarget(this.activeTarget);

        this.state = States.Initialized;
      } catch (e: any) {
        console.log('loading configuration from server failed: ' + e.message);
        this.state = States.NotInitialized;
        //AppNotInitializedError
        throw e;
      }
    },
    async loadAudiences() {
      try {
        const res = await getApi('audiences');
        this.audiences = res.data.results;
        this.state = States.Initialized;
      } catch (e: any) {
        this.state = States.NotInitialized;
        throw e;
      }
    },
  },
});
*/

// TODO: this implementation is more flexible than via Options API but it cause TS compilation errors,
// but the other one via Options API
export const configurationStore = defineStore('configuration', () => {
  const state = ref({
    state: States.Initial,
    debug: false,

    activeTarget: <string | undefined>'',

    name: '',
    name_org: '',
    is_new: false,

    targets: <ConfigTarget[]>[],

    project_id: '',
    ga4_loopback_window: '',
    ga4_loopback_recreate: false,
    bq_dataset_id: '',
    bq_dataset_location: '',
    ads_customer_id: '',
    ads_developer_token: '',
    ads_client_id: '',
    ads_client_secret: '',
    ads_refresh_token: '',
    ads_login_customer_id: '',

    ga4_project: '',
    ga4_dataset: '',
    ga4_table: '',

    days_ago_start: undefined, // TODO
    days_ago_end: undefined, // TODO

    stat: {
      app_ids: [] as string[],
      events: {} as Record<string, any[]>,
      countries: {} as Record<string, any[]>,
    },

    audiences: [] as AudienceInfo[],
    deletedAudiences: [] as AudienceInfo[],

    scheduled: undefined as boolean | undefined,
    schedule: '',
    schedule_timezone: '',
    schedule_email: <undefined | string>'',
  });

  const router = useRouter();

  function removeAudience(name: string) {
    const idx = this.audiences.findIndex((val: any) => val.name === name);
    if (idx >= 0) {
      const deleted = this.audiences.splice(idx, 1);
      this.deletedAudiences.push(...deleted);
    }
  }

  function initTarget(target: ConfigTarget) {
    this.name = target.name || '';
    this.name_org = target.name || '';
    this.bq_dataset_id = target.bq_dataset_id || '';
    this.bq_dataset_location = target.bq_dataset_location || '';
    this.ga4_project = target.ga4_project || '';
    this.ga4_dataset = target.ga4_dataset || '';
    this.ga4_table = target.ga4_table || '';
    this.ga4_loopback_window = target.ga4_loopback_window || '';
    this.ga4_loopback_recreate = !!target.ga4_loopback_recreate;
    this.ads_customer_id = target.ads_customer_id || '';
    this.ads_developer_token = target.ads_developer_token || '';
    this.ads_client_id = target.ads_client_id || '';
    this.ads_client_secret = target.ads_client_secret || '';
    this.ads_refresh_token = target.ads_refresh_token || '';
    this.ads_login_customer_id = target.ads_login_customer_id || '';
  }

  function activateTarget(name: string | undefined) {
    const target = this.targets.find((t) => t.name === name);
    if (target) {
      this.activeTarget = name;
      this.initTarget(target);

      setActiveTarget(name);
      router.push({ query: { target: name } });
    } else {
      console.error(`activateTarget: Target with name ${name} not found`);
    }
  }

  function switchTarget(name: string | undefined) {
    if (this.activeTarget != name) {
      this.activateTarget(name);
      window.setTimeout(() => {
        //alert(document.location.href);
        document.location.reload();
      }, 10);
    }
  }

  async function loadConfiguration() {
    try {
      console.log('loading configuration from server...');
      const res = await getApi('configuration');
      this.project_id = res.data.project_id;
      this.targets = res.data.targets || [];
      let target: ConfigTarget | undefined;

      const targetFromRoute = router.currentRoute.value.query.target;
      if (targetFromRoute) {
        this.activeTarget = <string>targetFromRoute;
      }
      if (this.activeTarget) {
        target = this.targets.find((t) => t.name === this.activeTarget);
      }
      if (!target && this.targets.length > 0) {
        target = this.targets[0];
        this.activeTarget = target.name;
      }
      if (!target) {
        target = {};
        this.activeTarget = '';
      }
      this.activateTarget(this.activeTarget);

      this.state = States.Initialized;
    } catch (e: any) {
      console.log('loading configuration from server failed: ' + e.message);
      this.state = States.NotInitialized;
      //AppNotInitializedError
      throw e;
    }
  }

  async function loadAudiences() {
    try {
      const res = await getApi('audiences');
      this.audiences = res.data.results;
      this.state = States.Initialized;
    } catch (e: any) {
      this.state = States.NotInitialized;
      //AppNotInitializedError
      throw e;
    }
  }

  return {
    ...state.value,
    removeAudience,
    initTarget,
    activateTarget,
    switchTarget,
    loadConfiguration,
    loadAudiences,
  };
});
