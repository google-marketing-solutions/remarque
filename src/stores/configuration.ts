import { defineStore } from 'pinia';
import { getApi } from 'boot/axios';

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
}
export enum States {
  Initial,
  Initialized,
  NotInitialized,
}
export const configurationStore = defineStore('configuration', {
  state: () => ({
    state: States.Initial,
    debug: false,

    name: '',
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

    scheduled: undefined as boolean|undefined,
    schedule: '',
    schedule_timezone: ''
  }),

  getters: {},

  actions: {
    removeAudience(name: string) {
      const idx = this.audiences.findIndex((val: any) => val.name === name);
      if (idx >= 0) {
        this.audiences.splice(idx, 1);
      }
    },
    async loadConfiguration() {
      const res = await getApi('configuration');
      this.name = res.data.name;
      this.project_id = res.data.project_id;
      this.bq_dataset_id = res.data.bq_dataset_id;
      this.bq_dataset_location = res.data.bq_dataset_location;
      this.ga4_project = res.data.ga4_project;
      this.ga4_dataset = res.data.ga4_dataset;
      this.ga4_table = res.data.ga4_table;
      this.ads_customer_id = res.data.ads_customer_id;
      this.ads_developer_token = res.data.ads_developer_token;
      this.ads_client_id = res.data.ads_client_id;
      this.ads_client_secret = res.data.ads_client_secret;
      this.ads_refresh_token = res.data.ads_refresh_token;
      this.ads_login_customer_id = res.data.ads_login_customer_id;
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
    // updateAudiencesStat(stat: Record<string, any>) {
    //   for (const audience of this.audiences) {
    //     const item = stat[audience.name];
    //     if (item) {
    //       audience.user_count = item.user_count;
    //     }
    //   }
    // },
  },
});
